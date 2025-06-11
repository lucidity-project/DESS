import pandas as pd
from dess.batch_inference import BatchInferencePipelineFactory


def test_dataset():
    df_llm = pd.read_parquet('storage/dataset/LLM_June2025_withSnippets.parquet')
    df_llm = df_llm.sample(100)
    pipeline = BatchInferencePipelineFactory.get_pipeline("gemini-2.5-flash-preview-05-20")

    # Create batch file
    batch_info = pipeline.prepare_batch_file(df_llm, 'llm_june_2025_batch001_test.jsonl')
    print("Batch file created")
    
    # Upload to GCS
    source_uri = pipeline.upload_batch_file(
        batch_info['file_path'],
        'dess-llm-jobs',
        'llm_june_2025_batch001_test.jsonl'
    )
    print("Batch file uploaded to GCS")

    # Create and run batch job
    output_uri = 'gs://dess-llm-jobs/output/'
    job = pipeline.create_batch_job(source_uri, output_uri)
    print("Batch job created")

    # Wait for completion
    completed_job = pipeline.wait_for_completion(job)
    print("Batch job completed")

    # Retrieve and merge results    
    df_llm = pipeline.retrieve_and_merge_results(completed_job, df_llm, batch_info)
    print(df_llm.head(5))
    print("Done")


if __name__=='__main__':
    test_dataset()