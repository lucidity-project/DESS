import pandas as pd
from llms.llm_factory import LLMFactory
import argparse

def populate_llm_column(
    df: pd.DataFrame,
    llm_type: str = "gemini"
) -> pd.DataFrame:
    """
    Populate department_llm column using LLM inference.

    Args:
        df (pd.DataFrame): DataFrame containing id_text and snippet columns
        llm_type (str): The type of LLM to use ("gemini" or "gpt").

    Returns:
        pd.DataFrame: Updated DataFrame with department_llm column populated.
    """
    # Validate required columns
    required_cols = ['id_text', 'snippet_1', 'snippet_2', 'snippet_3', 'snippet_4']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Initialize LLM
    llm = LLMFactory.get_llm(llm_type)
    if not llm.isOk():
        raise ValueError(f"Failed to initialize {llm_type} LLM")

    # Prepare input data
    input_data = []
    for idx, row in df.iterrows():
        # Get all non-empty snippets
        valid_snippets = [s for s in [row[f'snippet_{j}'] for j in range(1, 5)] 
                        if pd.notna(s) and str(s).strip()]
        
        if valid_snippets:  # Only include rows with valid snippets
            input_data.append({
                'idx': idx,
                'snippets': valid_snippets
            })

    # Get results from LLM
    try:
        results = llm.infer_departments_batch([item['snippets'] for item in input_data])
    except Exception as e:
        print(f"Error in LLM processing: {e}")
        results = ["ERROR"] * len(input_data)

    # Update department_llm column
    df['department_llm'] = None  # Initialize column
    for item, result in zip(input_data, results):
        df.at[item['idx'], 'department_llm'] = result

    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run inference using an LLM on a Parquet (.parquet) file")
    parser.add_argument("--llm_type", required=False, default="gemini", help="The type of LLM to use")
    parser.add_argument("--input_file", required=True, help="Path to the parquet (.parquet) file")
    args = parser.parse_args()
    
    # Read the input file
    df = pd.read_parquet(args.input_file)
    
    # Update DataFrame with LLM results
    df = populate_llm_column(df, llm_type=args.llm_type)
    
    # Print results
    print(df[["id_text", "department_llm"]])
