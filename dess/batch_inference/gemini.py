import pandas as pd
import os
import json
import uuid
import time
from typing import Dict, Any, Optional
from google.cloud import storage
from google import genai
from google.genai import types
from google.genai.types import CreateBatchJobConfig, JobState, HttpOptions
from dotenv import load_dotenv
from base import BatchInferencePipeline

class GeminiBatchInferencePipeline(BatchInferencePipeline):
    """
    Batch inference pipeline implementation for Google's Gemini models.
    """
    
    def __init__(self, model_name: str):
        """
        Initialize the pipeline with model name.
        
        Args:
            model_name (str): Gemini model name to use
        """
        load_dotenv()
        self.model_name = model_name
            
        # Initialize the client with Vertex AI settings
        self.client = genai.Client(http_options=types.HttpOptions(api_version='v1'))
        
    def prepare_batch_file(self, df: pd.DataFrame, output_file: str, prompt_file: Optional[str] = "prompts/default_department_extraction.txt") -> Dict[str, Any]:
        """
        Creates JSONL batch file in format required by Vertex AI batch predictions.
        
        Args:
            df (pd.DataFrame): DataFrame containing faculty data
            output_file (str): Path to save the JSONL file
            prompt_file (str, optional): Path to the prompt file.
            
        Returns:
            Dict[str, Any]: Information about the batch file including mapping between rows and prompts
        """
        # Validate required columns
        required_cols = ['id_text', 'snippet_1', 'snippet_2', 'snippet_3', 'snippet_4']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Create a new column for the combined text
        df['rawText'] = df.apply(lambda row: " ".join([row[f'snippet_{i}'] for i in range(1, 5) if pd.notna(row[f'snippet_{i}'])]), axis=1)
        df = df[df['rawText'].notna() & df['rawText'].str.strip()]
        
        # Read the prompt from file        
        with open(prompt_file, 'r') as f:
            prompt_template = f.read().strip()
        
        # Prepare batch ID and mapping
        batch_id = str(uuid.uuid4())
        row_mapping = []
        
        # Open file for writing
        with open(output_file, 'w') as f:
            for idx, row in df.iterrows():
                # Store mapping information
                row_mapping.append({
                    'idx': int(idx),
                    'id_text': row['id_text']
                })

                # Format according to Vertex AI batch prediction requirements
                instance = {
                    "request": {
                        "contents": [
                            {
                                "role": "user",
                                "parts": [{
                                    "text": f"""{prompt_template}
                                    
                                    Text: {row['rawText']}
                                    Department:"""
                                }]
                            }
                        ]
                    }
                }
                f.write(json.dumps(instance) + "\n")
        
        # Return batch information
        return {
            "batch_id": batch_id,
            "file_path": output_file,
            "model_name": self.model_name,
            "row_mapping": row_mapping,
            "row_count": len(row_mapping)
        }
        
    def upload_batch_file(self, file_path: str, bucket_name: str, destination_blob_name: Optional[str] = None) -> str:
        """
        Upload a file to Google Cloud Storage.
        
        Args:
            file_path (str): Path to the local file
            bucket_name (str): Name of the GCS bucket
            destination_blob_name (str, optional): Name of the destination blob
            
        Returns:
            str: GCS URI of the uploaded file (gs://bucket/path)
        """
        if destination_blob_name is None:
            destination_blob_name = os.path.basename(file_path)
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_filename(file_path)
        
        return f"gs://{bucket_name}/{destination_blob_name}"
        
    def create_batch_job(self, source_uri: str, output_uri: str) -> Any:
        """
        Creates batch job on Vertex AI.
        
        Args:
            source_uri (str): URI of the uploaded batch file
            output_uri (str): URI for the output results
            
        Returns:
            Any: Job reference object
        """
        job = self.client.batches.create(
            model=self.model_name,
            src=source_uri,
            config=CreateBatchJobConfig(dest=output_uri),
        )
        
        print(f"Job {job.name} | State: {job.state}")
        
        return job
        
    def get_batch_status(self, job_reference: Any) -> str:
        """
        Gets current status of batch job.
        
        Args:
            job_reference (Any): Job reference object
            
        Returns:
            str: Status of the batch job
        """
        job = self.client.batches.get(name=job_reference.name)
        print(f"Job state: {job.state}")
        return job.state.name
        
    def wait_for_completion(self, job_reference: Any, poll_interval: int = 30) -> Any:
        """
        Waits for batch job to complete.
        
        Args:
            job_reference (Any): Job reference object
            poll_interval (int): Time in seconds between status checks
            
        Returns:
            Any: Updated job reference object
        """
        completed_states = {
            JobState.JOB_STATE_SUCCEEDED,
            JobState.JOB_STATE_FAILED,
            JobState.JOB_STATE_CANCELLED,
            JobState.JOB_STATE_PAUSED,
        }
        
        job = job_reference
        while job.state not in completed_states:
            time.sleep(poll_interval)
            job = self.client.batches.get(name=job.name)
            print(f"Job state: {job.state}")
            
        return job
        
    def retrieve_and_merge_results(self, job_reference: Any, df: pd.DataFrame, mapping: Dict[str, Any]) -> pd.DataFrame:
        """
        Retrieves results from GCS and merges back into original dataframe.
        
        Args:
            job_reference (Any): Job reference object
            df (pd.DataFrame): Original DataFrame
            mapping (Dict[str, Any]): Mapping information from prepare_batch_file
            
        Returns:
            pd.DataFrame: Updated DataFrame with department_llm column populated
        """
        # TODO: Implement results retrieval from GCS
        # This would involve:
        # 1. Getting the output path from job_reference
        # 2. Downloading results from GCS
        # 3. Parsing results and mapping them back to the original dataframe
         # Extract output GCS path
        gcs_output_uri = job_reference.output_config['gcsDestination']['outputUriPrefix']  # Or adjust based on actual object structure

        # Initialize GCS client
        client = storage.Client()
        bucket_name, prefix = gcs_output_uri.replace("gs://", "").split("/", 1)
        bucket = client.bucket(bucket_name)

        # List blobs under output prefix
        blobs = list(bucket.list_blobs(prefix=prefix))
        result_blobs = [blob for blob in blobs if blob.name.endswith(".jsonl")]

        # Collect results from all JSONL files
        results = []
        for blob in result_blobs:
            content = blob.download_as_text()
            for line in content.strip().splitlines():
                data = json.loads(line)
                results.append(data.get('output', None))  # Adjust key if needed

        # Initialize the new column
        df['department_llm'] = None
        
        # Use mapping to assign results back to DataFrame
        row_mapping = mapping.get("row_mapping", [])
        for idx, result in zip(row_mapping, results):
            df.at[idx, 'department_llm'] = result

        return df


def extract_departments_from_vertex_batch(json_file_path, df=None):
    """
    Extract department names from Vertex AI batch output and add as a column to DataFrame.
    
    Parameters:
    json_file_path (str): Path to the JSON file containing Vertex AI batch output
    df (pandas.DataFrame, optional): Existing DataFrame to add departments to. 
                                   If None, creates a new DataFrame with departments only.
    
    Returns:
    pandas.DataFrame: DataFrame with departments added as a new column
    """
    departments = []
    
    # Read the JSON file
    with open(json_file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Split by lines since each JSON object is on a separate line
    lines = content.strip().split('\n')
    
    for line in lines:
        if line.strip():  # Skip empty lines
            try:
                # Parse each JSON object
                json_obj = json.loads(line)
                
                # Navigate to the department text in the response
                response = json_obj.get('response', {})
                candidates = response.get('candidates', [])
                
                if candidates:
                    content_obj = candidates[0].get('content', {})
                    parts = content_obj.get('parts', [])
                    
                    if parts:
                        # Extract the text which contains the department name
                        department_text = parts[0].get('text', '').strip()
                        # Remove any trailing newlines
                        department_text = department_text.rstrip('\n')
                        departments.append(department_text)
                    else:
                        departments.append('MISSING')
                else:
                    departments.append('MISSING')
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {e}")
                departments.append('MISSING')
            except Exception as e:
                print(f"Error processing line: {e}")
                departments.append('MISSING')
    
    # If no DataFrame provided, create a new one
    if df is None:
        df = pd.DataFrame()
    
    # Add departments as a new column
    df['department_llm'] = departments
    
    return df       
 
def test_main():
    pipeline = GeminiBatchInferencePipeline(model_name="gemini-2.0-flash-001")
    df_test  = pd.read_parquet('/Users/akhil/Desktop/RA-Scraping/DESS/storage/dataset/test_llm.parquet')

    # Create batch file
    batch_info = pipeline.prepare_batch_file(df_test, 'batch_requests.jsonl')
    print("Batch file created")
    
    # Upload to GCS
    source_uri = pipeline.upload_batch_file(
       batch_info['file_path'],
       'dess-llm-jobs',
       'batch_requests.jsonl'
   )
    print("Batch file uploaded to GCS")
   
    # Create and run batch job
    output_uri = 'gs://dess-llm-jobs/output/'
    job = pipeline.create_batch_job(source_uri, output_uri)
    print("Batch job created")
   
    # Wait for completion
    completed_job = pipeline.wait_for_completion(job)
    print("Batch job completed")

    # alt: open result file to unpack
    pipeline.retrieve_and_merge_results(completed_job, df_test, batch_info)
    print(df_test.head(5))
    
    
if __name__=='__main__':
    df_test  = pd.read_parquet('/Users/akhil/Desktop/RA-Scraping/DESS/storage/dataset/test_llm.parquet')
    json_path = '/Users/akhil/Desktop/RA-Scraping/DESS/dess/output_prediction-model-2025-05-19T02_41_34.174235Z_predictions.jsonl'
    #test_main()
    print(get_departments_list(json_path))