from dess.batch_inference.base import BatchInferencePipeline
from dess.batch_inference.gemini import GeminiBatchInferencePipeline
from dess.batch_inference.openai import OpenAIBatchInferencePipeline

class BatchInferencePipelineFactory:
    """
    Factory class for creating appropriate BatchInferencePipeline implementations
    based on model provider.
    """
    
    @staticmethod
    def get_pipeline(provider: str) -> BatchInferencePipeline:
        """
        Get the appropriate BatchInferencePipeline implementation based on provider.
        
        Args:
            provider (str): Provider name (e.g., "gemini", "openai")
            
        Returns:
            BatchInferencePipeline: The appropriate pipeline implementation
            
        Raises:
            ValueError: If the provider is not supported
        """
        if provider.split('-')[0] == "gemini":
            return GeminiBatchInferencePipeline(provider)
        elif provider.split('-')[0] == "gpt":
            return OpenAIBatchInferencePipeline(provider)
        else:
            raise ValueError(f"Unsupported model provider: {provider}") 