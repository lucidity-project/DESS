import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class BatchInferencePipeline(ABC):
    """
    Abstract base class for batch inference pipelines that process department extraction
    via LLMs in batch mode. Supports multiple model providers through concrete implementations.
    """
    
    @abstractmethod
    def prepare_batch_file(self, df: pd.DataFrame, output_file: str) -> Dict[str, Any]:
        """
        Creates batch file in format required by the provider.
        
        Args:
            df (pd.DataFrame): DataFrame containing faculty data
            output_file (str): Path to save the batch file
            
        Returns:
            Dict[str, Any]: Information about the batch file including mapping between rows and prompts
        """
        pass
        
    @abstractmethod
    def upload_batch_file(self, file_path: str, destination: str) -> str:
        """
        Uploads to appropriate storage, returns URI.
        
        Args:
            file_path (str): Path to the local batch file
            destination (str): Destination in storage service
            
        Returns:
            str: URI of the uploaded file
        """
        pass
        
    @abstractmethod
    def create_batch_job(self, source_uri: str, output_uri: str) -> Any:
        """
        Creates batch job on provider, returns job reference.
        
        Args:
            source_uri (str): URI of the uploaded batch file
            output_uri (str): URI for the output results
            
        Returns:
            Any: Job reference object
        """
        pass
        
    @abstractmethod
    def get_batch_status(self, job_reference: Any) -> str:
        """
        Gets current status of batch job.
        
        Args:
            job_reference (Any): Job reference object
            
        Returns:
            str: Status of the batch job
        """
        pass
        
    @abstractmethod
    def wait_for_completion(self, job_reference: Any, poll_interval: int = 30) -> Any:
        """
        Waits for batch job to complete.
        
        Args:
            job_reference (Any): Job reference object
            poll_interval (int): Interval in seconds to poll for job status
        Returns:
            Any: Job reference object
        """
        pass
        
    @abstractmethod
    def retrieve_and_merge_results(self, job_reference: Any, df: pd.DataFrame, mapping: Dict[str, Any]) -> pd.DataFrame:
        """
        Retrieves results and merges back into original dataframe.
        
        Args:
            job_reference (Any): Job reference object
            df (pd.DataFrame): Original DataFrame
            mapping (Dict[str, Any]): Mapping information from prepare_batch_file
            
        Returns:
            pd.DataFrame: Updated DataFrame with department_llm column populated
        """
        pass 