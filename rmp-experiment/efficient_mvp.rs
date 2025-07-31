use rateMyProfessorApi_rs::methods::RateMyProfessor;
use anyhow::Result;
use std::env;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    // Collect command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <university> <professor1> [professor2] [professor3] ...", args[0]);
        eprintln!("Example: {} \"Queens College\" \"Ross Greenberg\" \"Rebecca Nelson\"", args[0]);
        std::process::exit(1);
    }
    
    let university = &args[1];
    let professors: Vec<&String> = args[2..].iter().collect();
    
    println!("Fetching professor list for {} (this avoids rate limiting)...", university);
    
    // Create instance and get ALL professors from the university in ONE API call
    let mut rate_my_professor_instance = RateMyProfessor::construct_college(university);
    let professor_list = rate_my_professor_instance.get_professor_list().await?;
    
    println!("Found {} professors at {}", professor_list.len(), university);
    
    // Create a lookup map for efficient searching
    let mut name_to_department: HashMap<String, String> = HashMap::new();
    
    for prof in &professor_list {
        if let (Some(first), Some(last), Some(dept)) = (&prof.first_name, &prof.last_name, &prof.department) {
            let full_name = format!("{} {}", first, last).to_lowercase();
            name_to_department.insert(full_name, dept.clone());
        }
    }
    
    // Now search for each requested professor
    println!("\nSearching for requested professors:");
    for professor_name in professors {
        let search_name = professor_name.to_lowercase();
        
        if let Some(department) = name_to_department.get(&search_name) {
            println!("✓ {} -> Department: {}", professor_name, department);
        } else {
            // Try partial matching in case of slight name differences
            let mut found = false;
            for (stored_name, dept) in &name_to_department {
                if stored_name.contains(&search_name) || search_name.contains(stored_name) {
                    println!("~ {} -> Department: {} (partial match: {})", professor_name, dept, stored_name);
                    found = true;
                    break;
                }
            }
            if !found {
                println!("✗ {} -> Not found at {}", professor_name, university);
            }
        }
    }
    
    // Optional: Show all available professors for debugging
    if env::var("SHOW_ALL").is_ok() {
        println!("\nAll available professors:");
        for prof in &professor_list {
            if let (Some(first), Some(last), Some(dept)) = (&prof.first_name, &prof.last_name, &prof.department) {
                println!("  {} {} - {}", first, last, dept);
            }
        }
    }
    
    Ok(())
} 