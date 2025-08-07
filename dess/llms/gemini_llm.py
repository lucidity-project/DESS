from llms.llm_base import LLMBase
from google import genai
from typing import List
import os
from dotenv import load_dotenv

load_dotenv()

PROMPT = f"""Given the following text about a professor or faculty member, extract their department name.
        If no department is mentioned, return "MISSING". Only return the department name, nothing else.
        
        Text: {text}
        Department:"""

class GeminiLLM(LLMBase):
    """
    Gemini LLM model.
    """
    def __init__(self, model_name: str):
        self.model_name = model_name
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY environment variable not set")
        
        self.client = genai.Client(api_key=api_key)
        
    def get_response(self, prompt: str) -> str:
        """Get response for a single prompt."""
        response = self.llm.generate_content(prompt)
        return response.text
    
    def get_batch_responses(self, prompts: List[str]) -> List[str]:
        """Get responses for multiple prompts in batch."""
        responses = self.llm.generate_content(prompts)
        return [response.text for response in responses]
    
    def infer_department(self, text: str) -> str:
        """Infer department from text using a specific prompt."""
        
        return self.get_response(PROMPT)
    
    def infer_departments_batch(self, texts: List[str]) -> List[str]:
        """Infer departments from multiple texts in batch."""
        prompts = [
            f"""Given the following text about a professor or faculty member, extract their department name.
            If no department is mentioned, return "MISSING". Only return the department name, nothing else.
            
            Text: {text}
            Department:"""
            for text in texts
        ]
        
        return self.get_batch_responses(prompts)
    
    def isOk(self) -> bool:
        try:
            # Make a simple test request to check model availability
            test_prompt = "Hello"
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=test_prompt
            )
            return response.text is not None
        except Exception:
            return False

if __name__ == "__main__":
    llm = GeminiLLM("gemini-2.0-flash")
    print(llm.isOk())