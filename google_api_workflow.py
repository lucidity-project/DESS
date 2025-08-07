import os
import pandas as pd
import dess.nlp as nlp
import data_pipeline_manager as dpm
import cse
from dotenv import load_dotenv
import logging

# ========================================
# CONFIG
load_dotenv()
STORAGE_DIR = os.getenv('STORAGE_DIR')
ERROR_FILE = f'{STORAGE_DIR}/errors.csv'
FILE_PATH = f'{STORAGE_DIR}/dataset/shishir-toSearch-2025-02-11.parquet'
LOG_FILE = f'{STORAGE_DIR}/API_WORKFLOW_shishir.LOG'
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    force=True)
logger = logging.getLogger(__name__)
# ========================================

def _get_next_chunk_for_api_call():
    df = pd.read_parquet(FILE_PATH)
    unprocessed_df = df[df['isProcessed'] == False]
    
    # Limit rows per day based on rate limits
    rows_per_day = min(len(unprocessed_df), 100)
    today_df = unprocessed_df.iloc[:rows_per_day]
    
    logging.info(f"Selected {len(today_df)} rows for processing")
    return today_df

def end_to_end_workflow():
    # 1. Get today's chunk [constrained by rate limits and remaning count]
    df = _get_next_chunk_for_api_call()
    processed_ids = df['id_text'].tolist()
    
    # 2. Make API Calls
    logging.info("Starting Phase 1: Custom Search Engine API calls...")
    cse.populate_rawText_col(df)
    
    # Identify errors for logging purposes
    df_errors = df[df['rawText'].isna()]
    if not df_errors.empty:
        logging.info(f"Encountered {len(df_errors)} errors during API calls")
    
    write_header_error = not os.path.exists(ERROR_FILE)
    df_errors[['id_text']].to_csv(ERROR_FILE, mode='a', index=False, header=write_header_error)
    
    logging.info("[COMPLETE] Phase 1: API Calls")
    logging.info("Starting Phase 2: Department Extraction...")

    # 3. Run department extraction methodology
    df_non_errors = df.dropna(subset=['rawText']).copy().reset_index(drop=True) # Create a copy with reset index to avoid index mismatch issues
    nlp.extract_department_information(df_non_errors)

    logging.info("[COMPLETE] Phase 2: Populate Department Variables")

    # 4. Update out files
    dpm.update_parquet_file(df_non_errors, FILE_PATH, processed_ids)

    # 5. Cloud Sync and local cleanup
    logging.info("Starting Phase 3: Uploading to dropbox...")
    dbx = dpm.dropbox_oauth()
    dpm.push_new_dataset_files_to_dropbox(dbx)
    logging.info("[COMPLETE] Phase 3: Dropbox sync")
        
    # 6. Logging & Metrics
    processed_count = len(df_non_errors)
    logging.info(f"Processed {processed_count} rows. Error {len(df) - processed_count} rows.")
    
if __name__== "__main__":
    end_to_end_workflow()