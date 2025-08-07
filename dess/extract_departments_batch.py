import pandas as pd
import argparse
import os
from batch_inference.factory import BatchInferencePipelineFactory

def extract_departments_batch(
    input_file: str,
    provider: str = "gemini-2.0-flash-001",
    output_dir: str = None,
    bucket_name: str = "dess-llm-jobs"
):
    """
    Extract departments using batch inference pipeline.
    
    Args:
        input_file (str): Path to the parquet file
        provider (str): Model provider (e.g., "gemini-2.0-flash-001", "gpt")
        output_dir (str): Directory to save output files
        bucket_name (str): GCS bucket name (for Gemini)
    """
    # Get output directory
    if output_dir is None:
        output_dir = os.path.dirname(input_file)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Read input file
    df = pd.read_parquet(input_file)
    print(f"Read {len(df)} rows from {input_file}")
    
    # Get batch inference pipeline
    pipeline = BatchInferencePipelineFactory.get_pipeline(provider)
    
    # Prepare batch file
    batch_file = os.path.join(output_dir, f"batch_{os.path.basename(input_file).split('.')[0]}.jsonl")
    batch_info = pipeline.prepare_batch_file(df, batch_file)
    print(f"Prepared batch file with {batch_info['row_count']} rows")
    
    # Upload batch file
    if provider.startswith("gemini"):
        source_uri = pipeline.upload_batch_file(batch_file, bucket_name)
        print(f"Uploaded batch file to {source_uri}")
        
        # Set output URI
        output_uri = f"gs://{bucket_name}"
    else:
        # For OpenAI, we don't need to upload
        source_uri = batch_file
        output_uri = os.path.join(output_dir, f"output_{os.path.basename(input_file).split('.')[0]}")
        
    # Create batch job
    job = pipeline.create_batch_job(source_uri, output_uri)
    print(f"Created batch job")
    
    # Wait for job to complete if it's OpenAI
    if provider == "gpt":
        job = pipeline.process_batch_job(job)
    
    # Poll status
    status = pipeline.get_batch_status(job)
    print(f"Job status: {status}")
    
    # If it's Gemini, wait for job to complete
    if provider.startswith("gemini"):
        job = pipeline.wait_for_completion(job)
        print(f"Job completed with status: {job.state}")
    
    # Retrieve and merge results
    df_updated = pipeline.retrieve_and_merge_results(job, df, batch_info)
    print(f"Retrieved and merged results")
    
    # Save output
    output_file = os.path.join(output_dir, f"processed_{os.path.basename(input_file)}")
    df_updated.to_parquet(output_file)
    print(f"Saved results to {output_file}")
    
    return df_updated

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract departments using batch inference pipeline")
    parser.add_argument("--input_file", required=True, help="Path to the parquet file")
    parser.add_argument("--provider", default="gemini-2.0-flash-001", help="Model provider")
    parser.add_argument("--output_dir", help="Directory to save output files")
    parser.add_argument("--bucket_name", default="dess-llm-jobs", help="GCS bucket name (for Gemini)")
    
    args = parser.parse_args()
    
    extract_departments_batch(
        input_file=args.input_file,
        provider=args.provider,
        output_dir=args.output_dir,
        bucket_name=args.bucket_name
    ) 