from llms.llm_base import LLMBase
import openai
from typing import List
import os
from dotenv import load_dotenv

load_dotenv()

class GPTLLM(LLMBase):
    """
    OpenAI GPT LLM model.
    """
    def __init__(self, model_name: str):
        self.model_name = model_name
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        
        openai.api_key = api_key
        self.llm = openai.ChatCompletion
        
    def get_response(self, prompt: str) -> str:
        """Get response for a single prompt."""
        response = self.llm.create(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content.strip()
    
    def get_batch_responses(self, prompts: List[str]) -> List[str]:
        """Get responses for multiple prompts in batch."""
        responses = []
        for prompt in prompts:
            response = self.get_response(prompt)
            responses.append(response)
        return responses
    
    def infer_department(self, text: str) -> str:
        """Infer department from text using a specific prompt."""
        prompt = f"""Given the following text about a professor or faculty member, extract their department name.
        If no department is mentioned, return "MISSING". Only return the department name, nothing else.
        
        Text: {text}
        Department:"""
        
        return self.get_response(prompt)
    
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
