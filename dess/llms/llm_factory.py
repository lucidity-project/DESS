from llms.gemini_llm import GeminiLLM
from llms.gpt_llm import GPTLLM

class LLMFactory:
    """
    This class creates and returns the right LLM object.
    """
    @staticmethod
    def get_llm(llm_type: str):
        if llm_type.split('-')[0] == "gemini":
            return GeminiLLM(llm_type)
        elif llm_type == "gpt":
            return GPTLLM(llm_type)
        else:
            raise ValueError(f"Invalid LLM type: {llm_type}")