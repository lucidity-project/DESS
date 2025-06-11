import pandas as pd
import os
import json
import uuid
from typing import Dict, Any, List, Optional
from openai import OpenAI
from dotenv import load_dotenv
from dess.batch_inference.base import BatchInferencePipeline

class OpenAIBatchInferencePipeline(BatchInferencePipeline):
    """
    Batch inference pipeline implementation for OpenAI models.
    """
    
    def __init__(self, model_name: str):
        """
        Initialize the pipeline with model name.
        
        Args:
            model_name (str): OpenAI model name to use
        """
        load_dotenv()
        self.model_name = model_name
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        
        self.client = OpenAI(api_key=api_key)
        self.batch_jobs = {}  # Keep track of batch jobs
        
    def prepare_batch_file(self, df: pd.DataFrame, output_file: str) -> Dict[str, Any]:
        """
        Creates JSONL batch file in format required by OpenAI API.
        
        Args:
            df (pd.DataFrame): DataFrame containing faculty data
            output_file (str): Path to save the JSONL file
            
        Returns:
            Dict[str, Any]: Information about the batch file including mapping between rows and prompts
        """
        # TODO: This is definitely wrong. Generate the desired JSONL file format (see here: https://platform.openai.com/docs/guides/batch#1-prepare-your-batch-file)
        # Validate required columns
        required_cols = ['id_text', 'snippet_1', 'snippet_2', 'snippet_3', 'snippet_4']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Create a new column for the combined text
        df['rawText'] = df.apply(lambda row: " ".join([row[f'snippet_{i}'] for i in range(1, 5) if pd.notna(row[f'snippet_{i}'])]), axis=1)
        df = df[df['rawText'].notna() & df['rawText'].str.strip()]
        
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

                # OpenAI format is different from Gemini
                instance = {
                    "model": self.model_name,
                    "messages": [
                        {
                            "role": "user",
                            "content": f"""Given the following text about a professor or faculty member, extract their department name.
                            If no department is mentioned, return "MISSING". Only return the department name, nothing else.

                            Text: {row['rawText']}
                            Department:"""
                        }
                    ]
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
        
    def upload_batch_file(self, file_path: str, destination: str) -> str:
        """
        For OpenAI, this is a no-op as OpenAI doesn't use cloud storage for batch jobs.
        We just return the local file path.
        
        Args:
            file_path (str): Path to the local file
            destination (str): Not used for OpenAI
            
        Returns:
            str: Local file path
        """
        # OpenAI doesn't use cloud storage for batch jobs, but we
        # keep the interface consistent with other implementations
        # TODO: See here maybe: https://platform.openai.com/docs/guides/batch#2-upload-your-batch-input-file
        return file_path
        
    def create_batch_job(self, source_uri: str, output_uri: str) -> Any:
        """
        Creates a simulated batch job for OpenAI by processing in chunks.

        Following instructions from here: https://platform.openai.com/docs/guides/batch#3-create-the-batch
        
        Args:
            source_uri (str): Path to the JSONL file
            output_uri (str): Path for the output results
            
        Returns:
            Any: Job reference object (for OpenAI, this is a dictionary with batch info)
        """
        
        batch_id = str(uuid.uuid4())
        
        job_reference = self.client.batches.create(
            input_file_id=batch_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={
                "description": "batch job for department extraction"
            }
        )
        
        # Store job reference
        self.batch_jobs[batch_id] = job_reference
        
        print(f"Created batch job: {batch_id}")
        print(f"Status: {job_reference['status']}")
        
        return job_reference
        
    def get_batch_status(self, job_reference: Dict[str, Any]) -> str:
        """
        Gets current status of batch job.
        
        Args:
            job_reference (Dict[str, Any]): Job reference dictionary
            
        Returns:
            str: Status of the batch job
        """
        batch_id = job_reference["id"]
        if batch_id in self.batch_jobs:
            return self.batch_jobs[batch_id]["status"]
        return "NOT_FOUND"
        
    def process_batch_job(self, job_reference: Dict[str, Any], batch_size: int = 10) -> Dict[str, Any]:
        """
        Process a batch job by sending requests to OpenAI API in chunks.
        
        Args:
            job_reference (Dict[str, Any]): Job reference dictionary
            batch_size (int): Number of requests to send at once
            
        Returns:
            Dict[str, Any]: Updated job reference dictionary
        """
        # TODO: This is probably wrong.
        batch_id = job_reference["batch_id"]
        source_file = job_reference["source_file"]
        
        # Update status
        self.batch_jobs[batch_id]["status"] = "RUNNING"
        
        # Read JSONL file
        with open(source_file, 'r') as f:
            lines = f.readlines()
        
        # Process in batches
        results = []
        for i in range(0, len(lines), batch_size):
            batch = lines[i:i+batch_size]
            batch_requests = [json.loads(line) for line in batch]
            
            # Process each request
            batch_results = []
            for request in batch_requests:
                try:
                    response = self.client.chat.completions.create(**request)
                    result = response.choices[0].message.content
                    batch_results.append(result)
                except Exception as e:
                    print(f"Error processing request: {e}")
                    batch_results.append("ERROR")
            
            results.extend(batch_results)
            
            # Print progress
            print(f"Processed {len(results)}/{len(lines)} requests")
        
        # Update job reference
        self.batch_jobs[batch_id]["results"] = results
        self.batch_jobs[batch_id]["status"] = "SUCCEEDED"
        
        return self.batch_jobs[batch_id]
        
    def retrieve_and_merge_results(self, job_reference: Dict[str, Any], df: pd.DataFrame, mapping: Dict[str, Any]) -> pd.DataFrame:
        """
        Retrieves results and merges back into original dataframe.
        
        Args:
            job_reference (Dict[str, Any]): Job reference dictionary
            df (pd.DataFrame): Original DataFrame
            mapping (Dict[str, Any]): Mapping information from prepare_batch_file
            
        Returns:
            pd.DataFrame: Updated DataFrame with department_llm column populated
        """
        # TODO: This is probably/definitely wrong.
        # Check if job is completed
        batch_id = job_reference["batch_id"]
        if self.batch_jobs[batch_id]["status"] != "SUCCEEDED":
            # Process job if not done
            self.process_batch_job(job_reference)
        
        # Get results
        results = self.batch_jobs[batch_id]["results"]
        row_mapping = mapping.get("row_mapping", [])
        
        # Initialize department_llm column
        df['department_llm'] = None
        
        # Update department_llm column
        for i, result in enumerate(results):
            if i < len(row_mapping):
                idx = row_mapping[i]['idx']
                df.at[idx, 'department_llm'] = result
        
        return df 