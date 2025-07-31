use rateMyProfessorApi_rs::methods::RateMyProfessor;
use anyhow::Result;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Collect command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <university> <professor name>", args[0]);
        std::process::exit(1);
    }
    let university = &args[1];
    let professor = &args[2];


    let mut rate_my_professor_instance = RateMyProfessor::construct_college_and_professor(university, professor);
    let teacher_summary = rate_my_professor_instance.get_teacher_summary_and_save(false, "Teacher_Summary.json").await?;
    println!("{teacher_summary:#?}");

    Ok(())
}