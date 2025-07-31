use rateMyProfessorApi_rs::methods::RateMyProfessor;
use anyhow::Result;
use std::env;
use std::collections::HashMap;
use std::fs::File;
use std::time::{Duration, Instant};
use arrow::array::{Array, StringArray, ArrayRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

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



/// Process universities with actual API calls
async fn process_universities(
    queries_by_university: HashMap<String, Vec<ProfessorQuery>>
) -> Result<Vec<ProfessorResult>> {
    let mut stats = ProcessingStats::new();
    stats.total_rows = queries_by_university.values().map(|v| v.len()).sum();
    stats.unique_universities = queries_by_university.len();
    
    println!("\n🚀 Starting API processing...");
    println!("📊 Total rows: {}", stats.total_rows);
    println!("🏫 Unique universities: {}", stats.unique_universities);
    println!("⏱️  Starting at: {:?}", stats.start_time);
    
    let mut all_results: Vec<ProfessorResult> = Vec::new();
    let mut processed_count = 0;
    
    // Process each university once
    for (university, university_queries) in queries_by_university {
        stats.print_progress(processed_count, &university);
        
        let api_start = Instant::now();
        
        // Single API call per university
        let mut rate_my_professor_instance = RateMyProfessor::construct_college(&university);
        stats.api_calls_made += 1;
        
        match rate_my_professor_instance.get_professor_list().await {
            Ok(professor_list) => {
                let api_duration = api_start.elapsed();
                println!("   ✅ Found {} professors in {:?}", professor_list.len(), api_duration);
                
                // Build lookup map for this university
                let mut name_to_department: HashMap<String, String> = HashMap::new();
                for prof in &professor_list {
                    if let (Some(first), Some(last), Some(dept)) = (&prof.first_name, &prof.last_name, &prof.department) {
                        let full_name = format!("{} {}", first, last).to_lowercase();
                        name_to_department.insert(full_name, dept.clone());
                    }
                }
                
                // Process all queries for this university
                let mut local_matches = 0;
                let total_queries_for_university = university_queries.len();
                for query in university_queries {
                    let search_name = query.professor_name.to_lowercase();
                    let department_rmp = name_to_department.get(&search_name).cloned()
                        .or_else(|| {
                            // Try partial matching
                            for (stored_name, dept) in &name_to_department {
                                if stored_name.contains(&search_name) || search_name.contains(stored_name) {
                                    return Some(dept.clone());
                                }
                            }
                            None
                        });
                    
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
                
                println!("   📊 Matched {}/{} professors from this university", 
                         local_matches, total_queries_for_university);
            }
            Err(e) => {
                println!("   ❌ Error fetching professors: {}", e);
                
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
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
    
    // Final statistics
    println!("\n📈 Processing Complete!");
    println!("⏱️  Total time: {:?}", stats.elapsed());
    println!("🎯 Success rate: {}/{} ({:.1}%)", 
             stats.successful_matches, stats.total_rows,
             (stats.successful_matches as f64 / stats.total_rows as f64) * 100.0);
    println!("🌐 API calls made: {}", stats.api_calls_made);
    
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
    
    println!("🚀 Starting processing...");
    if let Some(limit) = max_rows {
        println!("   Limiting to {} rows for testing", limit);
    }
    
    // Step 1: Read parquet file
    let queries = read_parquet_file(file_path, max_rows)?;
    
    if queries.is_empty() {
        println!("❌ No valid queries found in file");
        return Ok(());
    }
    
    // Step 2: Group by university
    let queries_by_university = group_queries_by_university(queries);
    
    let total_rows = queries_by_university.values().map(|v| v.len()).sum::<usize>();
    println!("\n📋 Ready to process {} rows across {} universities", total_rows, queries_by_university.len());
    
    // Show top 3 universities by professor count
    let mut sorted_unis: Vec<_> = queries_by_university.iter().collect();
    sorted_unis.sort_by_key(|(_, profs)| std::cmp::Reverse(profs.len()));
    for (university, profs) in sorted_unis.iter().take(3) {
        println!("   {} → {} professors", university, profs.len());
    }
    if queries_by_university.len() > 3 {
        println!("   ... and {} more universities", queries_by_university.len() - 3);
    }
    
    // Step 3: Process with API calls
    let results = process_universities(queries_by_university).await?;
    
    // Step 4: Write back to file
    write_results_to_file(file_path, results)?;
    
    println!("\n🎉 Complete! File {} has been updated with department_rmp column.", file_path);
    
    println!("⏱️  Total processing time: {:?}", total_start.elapsed());
    
    Ok(())
} 