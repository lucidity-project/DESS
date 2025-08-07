use anyhow::Result;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use log::{info, warn};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::file::properties::WriterProperties;
use rateMyProfessorApi_rs::methods::RateMyProfessor;
use sanitize_filename::sanitize;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CACHE_DIR: &str = "storage/rmp-cache";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfessorList {
    /// Unique ID associated with a particular professor.
    pub id: Option<String>,
    /// Old unique ID that was used to identify a particular professor.
    pub legacy_id: Option<String>,
    /// First name of the Professor.
    pub first_name: Option<String>,
    /// Last name of the Professor.
    pub last_name: Option<String>,
    /// Department which the professor is affiliated with.
    pub department: Option<String>,
    /// Floating point value ranging from `1.0-5.0` on a scale of satisfaction.
    pub avg_rating: Option<f64>,
    /// Number of students that have provided feedback on this professor.
    pub num_rating: Option<i32>,
    /// Floating point value between `1.0-5.0` representing the difficulty level o
    pub avg_difficulty: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PageInfo {
    #[serde(rename = "hasNextPage")]
    has_next_page: bool,
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProfessorNode {
    node: ProfessorList,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProfessorEdges {
    edges: Vec<ProfessorNode>,
    #[serde(rename = "pageInfo")]
    page_info: PageInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TeacherSearch {
    teachers: ProfessorEdges,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NewSearch {
    search: TeacherSearch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResponseData {
    data: NewSearch,
}

pub const TEACHER_LIST_QUERY: &str = r#"query TeacherSearchResultsPageQuery(
        $query: TeacherSearchQuery!
        $count: Int!
        $cursor: String
    ) {
        newSearch {
            teachers(query: $query, first: $count, after: $cursor) {
                edges {
                    node {
                        id
                        legacyId
                        firstName
                        lastName
                        department
                        avgRating
                        numRatings
                        wouldTakeAgainPercent
                        avgDifficulty
                        school {
                            name
                            id
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
                resultCount
            }
        }
    }"#;

#[derive(Debug, Clone)]
struct ProfessorQuery {
    university: String,
    professor_name: String,
    firstname: String,
    lastname: String,
    row_index: usize,
}

#[derive(Debug)]
struct ProfessorResult {
    university: String,
    firstname: String,
    lastname: String,
    department_rmp: Option<String>,
    row_index: usize,
}

#[derive(Debug)]
struct ProcessingStats {
    total_rows: usize,
    unique_universities: usize,
    api_calls_made: usize,
    successful_matches: usize,
    start_time: Instant,
}

impl ProcessingStats {
    fn new() -> Self {
        Self {
            total_rows: 0,
            unique_universities: 0,
            api_calls_made: 0,
            successful_matches: 0,
            start_time: Instant::now(),
        }
    }
    
    fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
    
    fn estimate_remaining(&self, processed_universities: usize) -> Option<Duration> {
        if processed_universities == 0 {
            return None;
        }
        
        let elapsed = self.elapsed();
        let rate = elapsed.as_secs_f64() / processed_universities as f64;
        let remaining = self.unique_universities - processed_universities;
        
        Some(Duration::from_secs_f64(rate * remaining as f64))
    }
    
    fn print_progress(&self, processed_universities: usize, current_university: &str) {
        let elapsed = self.elapsed();
        let progress = (processed_universities as f64 / self.unique_universities as f64) * 100.0;
        
        print!("🏫 [{}/{}] {:.1}% | {:?} elapsed", 
               processed_universities, self.unique_universities, progress, elapsed);
        
        if let Some(eta) = self.estimate_remaining(processed_universities) {
            print!(" | ETA: {:?}", eta);
        }
        
        println!(" | Processing: {}", current_university);
    }
}

/// Read parquet file and extract professor queries
fn read_parquet_file(file_path: &str, max_rows: Option<usize>) -> Result<Vec<ProfessorQuery>> {
    let read_start = Instant::now();
    
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_reader = builder.build()?;
    
    let mut queries: Vec<ProfessorQuery> = Vec::new();
    
    // Read all record batches
    for batch_result in arrow_reader {
        let batch = batch_result?;
        
        // Get column indices
        let schema = batch.schema();
        let firstname_idx = schema.index_of("firstname")?;
        let lastname_idx = schema.index_of("lastname")?;
        let university_idx = schema.index_of("university")?;
        
        // Extract string arrays
        let firstname_array = batch.column(firstname_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("firstname column is not a string array"))?;
        
        let lastname_array = batch.column(lastname_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("lastname column is not a string array"))?;
            
        let university_array = batch.column(university_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("university column is not a string array"))?;
        
        // Process each row
        for i in 0..batch.num_rows() {
            // Check if any values are null
            if !firstname_array.is_null(i) && !lastname_array.is_null(i) && !university_array.is_null(i) {
                let firstname = firstname_array.value(i);
                let lastname = lastname_array.value(i);
                let university = university_array.value(i);
                
                queries.push(ProfessorQuery {
                    university: university.to_string(),
                    professor_name: format!("{} {}", firstname, lastname),
                    firstname: firstname.to_string(),
                    lastname: lastname.to_string(),
                    row_index: queries.len(),
                });
                
                // Check if we've hit the row limit
                if let Some(limit) = max_rows {
                    if queries.len() >= limit {
                        break;
                    }
                }
            }
        }
        
        // Check limit after each batch
        if let Some(limit) = max_rows {
            if queries.len() >= limit {
                break;
            }
        }
    }
    
    println!("📖 Read {} queries in {:?}", queries.len(), read_start.elapsed());
    Ok(queries)
}

/// Group queries by university for efficient processing
fn group_queries_by_university(queries: Vec<ProfessorQuery>) -> HashMap<String, Vec<ProfessorQuery>> {
    let group_start = Instant::now();
    
    let mut queries_by_university: HashMap<String, Vec<ProfessorQuery>> = HashMap::new();
    for query in queries {
        queries_by_university
            .entry(query.university.clone())
            .or_insert_with(Vec::new)
            .push(query);
    }
    
    println!("📊 Grouped into {} universities in {:?}", 
             queries_by_university.len(), group_start.elapsed());
    
    queries_by_university
}



use reqwest::header::{HeaderMap, HeaderValue};

pub const API_LINK: &str = "https://www.ratemyprofessors.com/graphql"; // base URL

pub fn get_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "User-Agent",
        HeaderValue::from_static(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
        ),
    );
    headers.insert("Accept", HeaderValue::from_static("*/*"));
    headers.insert("Accept-Language", HeaderValue::from_static("en-US,en;q=0.5"));
    headers.insert(
        "Content-Type",
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_static("Basic dGVzdDp0ZXN0"),
    );
    headers.insert("Sec-GPC", HeaderValue::from_static("1"));
    headers.insert("Sec-Fetch-Dest", HeaderValue::from_static("empty"));
    headers.insert("Sec-Fetch-Mode", HeaderValue::from_static("cors"));
    headers.insert("Sec-Fetch-Site", HeaderValue::from_static("same-origin"));
    headers.insert("Priority", HeaderValue::from_static("u=4"));
    headers
}

async fn get_professor_list_by_school_paginated(college_id: &str) -> Result<Vec<ProfessorList>> {
    let mut all_professors: Vec<ProfessorList> = Vec::new();
    let mut has_next_page = true;
    let mut cursor: Option<String> = None;
    let client = reqwest::Client::new();

    while has_next_page {
        let variables = serde_json::json!({
            "query": {
                "text": "",
                "schoolID": college_id,
                "fallback": true,
                "departmentID": null,
            },
            "count": 1000,
            "cursor": cursor,
        });

        let payload = serde_json::json!({
            "query": TEACHER_LIST_QUERY,
            "variables": variables,
        });

        let response = client
            .post(API_LINK)
            .headers(get_headers())
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Network response from RMP not OK"));
        }

        let response_data: ResponseData = response.json().await?;
        let search_results = response_data.data.search;
        let page_info = search_results.teachers.page_info;

        for edge in search_results.teachers.edges {
            all_professors.push(edge.node);
        }

        has_next_page = page_info.has_next_page;
        cursor = page_info.end_cursor;
    }

    Ok(all_professors)
}

async fn get_or_cache_professor_list(
    university: &str,
    stats: &mut ProcessingStats,
) -> Result<Vec<ProfessorList>> {
    let file_name = sanitize(university);
    let cache_path = Path::new(CACHE_DIR).join(format!("{}.json", file_name));

    if let Ok(file_content) = fs::read_to_string(&cache_path) {
        if let Ok(professor_list) = serde_json::from_str(&file_content) {
            info!("   CACHE HIT for {}", university);
            return Ok(professor_list);
        }
    }

    info!("   CACHE MISS for {}", university);
    let api_start = Instant::now();
    stats.api_calls_made += 1;

    let mut rmp = RateMyProfessor::construct_college(university);
    let schools = rmp.get_college_info().await?;
    let school = schools
        .first()
        .ok_or_else(|| anyhow::anyhow!("No school found with name {}", university))?;
    let school_id = &school.node.id;

    let professor_list = get_professor_list_by_school_paginated(school_id).await?;

    let api_duration = api_start.elapsed();
    info!(
        "   ✅ Found {} professors in {:?}",
        professor_list.len(),
        api_duration
    );

    let file_content = serde_json::to_string_pretty(&professor_list)?;
    fs::write(&cache_path, file_content)?;
    info!("   CACHED to {:?}", cache_path);

    Ok(professor_list)
}

/// Process universities with actual API calls
async fn process_universities(
    queries_by_university: HashMap<String, Vec<ProfessorQuery>>,
) -> Result<Vec<ProfessorResult>> {
    let mut stats = ProcessingStats::new();
    stats.total_rows = queries_by_university.values().map(|v| v.len()).sum();
    stats.unique_universities = queries_by_university.len();

    info!("🚀 Starting API processing...");
    info!("📊 Total rows: {}", stats.total_rows);
    info!("🏫 Unique universities: {}", stats.unique_universities);
    info!("⏱️  Starting at: {:?}", stats.start_time);

    let mut all_results: Vec<ProfessorResult> = Vec::new();
    let mut processed_count = 0;

    // Process each university once
    for (university, university_queries) in queries_by_university {
        stats.print_progress(processed_count, &university);

        match get_or_cache_professor_list(&university, &mut stats).await {
            Ok(professor_list) => {
                // Build lookup map for this university
                let mut name_to_department: HashMap<String, String> = HashMap::new();
                for prof in &professor_list {
                    if let (Some(first), Some(last), Some(dept)) =
                        (&prof.first_name, &prof.last_name, &prof.department)
                    {
                        let full_name = format!("{} {}", first, last).to_lowercase();
                        name_to_department.insert(full_name, dept.clone());
                    }
                }

                // Process all queries for this university
                let mut local_matches = 0;
                let total_queries_for_university = university_queries.len();
                for query in university_queries {
                    let search_name = query.professor_name.to_lowercase();
                    let department_rmp = name_to_department.get(&search_name).cloned().or_else(
                        || {
                            // Try partial matching
                            for (stored_name, dept) in &name_to_department {
                                if stored_name.contains(&search_name)
                                    || search_name.contains(stored_name)
                                {
                                    return Some(dept.clone());
                                }
                            }
                            None
                        },
                    );

                    if department_rmp.is_some() {
                        local_matches += 1;
                        stats.successful_matches += 1;
                    }

                    all_results.push(ProfessorResult {
                        university: query.university,
                        firstname: query.firstname,
                        lastname: query.lastname,
                        department_rmp,
                        row_index: query.row_index,
                    });
                }

                info!(
                    "   📊 Matched {}/{} professors from this university",
                    local_matches, total_queries_for_university
                );
            }
            Err(e) => {
                warn!("   ❌ Error fetching professors: {}", e);

                // Add error results for all queries from this university
                for query in university_queries {
                    all_results.push(ProfessorResult {
                        university: query.university,
                        firstname: query.firstname,
                        lastname: query.lastname,
                        department_rmp: None,
                        row_index: query.row_index,
                    });
                }
            }
        }

        processed_count += 1;

        // Add a small delay to be respectful to the API
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Final statistics
    info!("\n📈 Processing Complete!");
    info!("⏱️  Total time: {:?}", stats.elapsed());
    info!(
        "🎯 Success rate: {}/{} ({:.1}%)",
        stats.successful_matches,
        stats.total_rows,
        (stats.successful_matches as f64 / stats.total_rows as f64) * 100.0
    );
    info!("🌐 API calls made: {}", stats.api_calls_made);

    Ok(all_results)
}

/// Write results back to the same parquet file (in-place modification)
fn write_results_to_file(file_path: &str, results: Vec<ProfessorResult>) -> Result<()> {
    println!("\n💾 Writing results back to: {}", file_path);
    let write_start = Instant::now();
    
    // Sort results by original row index to maintain input order
    let mut sorted_results = results;
    sorted_results.sort_by_key(|r| r.row_index);
    
    // Prepare output data
    let firstname_values: Vec<Option<&str>> = sorted_results.iter().map(|r| Some(r.firstname.as_str())).collect();
    let lastname_values: Vec<Option<&str>> = sorted_results.iter().map(|r| Some(r.lastname.as_str())).collect();
    let university_values: Vec<Option<&str>> = sorted_results.iter().map(|r| Some(r.university.as_str())).collect();
    let department_values: Vec<Option<&str>> = sorted_results.iter().map(|r| r.department_rmp.as_deref()).collect();
    
    // Create arrays
    let firstname_array: ArrayRef = Arc::new(StringArray::from_iter(firstname_values));
    let lastname_array: ArrayRef = Arc::new(StringArray::from_iter(lastname_values));
    let university_array: ArrayRef = Arc::new(StringArray::from_iter(university_values));
    let department_array: ArrayRef = Arc::new(StringArray::from_iter(department_values));
    
    // Create schema (include department_rmp column)
    let schema = Arc::new(Schema::new(vec![
        Field::new("firstname", DataType::Utf8, false),
        Field::new("lastname", DataType::Utf8, false),
        Field::new("university", DataType::Utf8, false),
        Field::new("department_rmp", DataType::Utf8, true),
    ]));
    
    // Create record batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![firstname_array, lastname_array, university_array, department_array],
    )?;
    
    // Write to parquet file (overwrites original)
    let output_file_handle = File::create(file_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(output_file_handle, schema, Some(props))?;
    
    writer.write(&batch)?;
    writer.close()?;
    
    println!("✅ Wrote {} rows in {:?}", sorted_results.len(), write_start.elapsed());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let total_start = Instant::now();
    env_logger::init();

    // Create cache directory if it doesn't exist
    fs::create_dir_all(CACHE_DIR)?;

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 || args.len() > 3 {
        eprintln!("Usage: {} <parquet_file> [max_rows]", args[0]);
        eprintln!("");
        eprintln!("Arguments:");
        eprintln!("  parquet_file    Input parquet file (will be modified in-place)");
        eprintln!("  max_rows        Optional: limit processing to first N rows (for testing)");
        eprintln!("");
        eprintln!("Required columns: firstname, lastname, university");
        eprintln!("Will add column: department_rmp");
        eprintln!("");
        eprintln!("Examples:");
        eprintln!("  {} faculty.parquet           # Process all rows", args[0]);
        eprintln!("  {} faculty.parquet 100       # Process first 100 rows only", args[0]);
        std::process::exit(1);
    }

    let file_path = &args[1];
    let max_rows = args.get(2).and_then(|s| s.parse::<usize>().ok());

    info!("🚀 Starting processing...");
    if let Some(limit) = max_rows {
        info!("   Limiting to {} rows for testing", limit);
    }

    // Step 1: Read parquet file
    let queries = read_parquet_file(file_path, max_rows)?;

    if queries.is_empty() {
        warn!("❌ No valid queries found in file");
        return Ok(());
    }

    // Step 2: Group by university
    let queries_by_university = group_queries_by_university(queries);

    let total_rows = queries_by_university
        .values()
        .map(|v| v.len())
        .sum::<usize>();
    info!(
        "\n📋 Ready to process {} rows across {} universities",
        total_rows,
        queries_by_university.len()
    );

    // Show top 3 universities by professor count
    let mut sorted_unis: Vec<_> = queries_by_university.iter().collect();
    sorted_unis.sort_by_key(|(_, profs)| std::cmp::Reverse(profs.len()));
    for (university, profs) in sorted_unis.iter().take(3) {
        info!("   {} → {} professors", university, profs.len());
    }
    if queries_by_university.len() > 3 {
        info!(
            "   ... and {} more universities",
            queries_by_university.len() - 3
        );
    }

    // Step 3: Process with API calls
    let results = process_universities(queries_by_university).await?;

    // Step 4: Write back to file
    write_results_to_file(file_path, results)?;

    info!(
        "\n🎉 Complete! File {} has been updated with department_rmp column.",
        file_path
    );

    info!("⏱️  Total processing time: {:?}", total_start.elapsed());

    Ok(())
} 