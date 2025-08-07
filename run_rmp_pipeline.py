import os
import pandas as pd
import subprocess
import dropbox
from dotenv import load_dotenv
from data_pipeline_manager import dropbox_oauth, import_files_from_dropbox, upload_large_file

load_dotenv()

STORAGE_DIR = os.getenv("STORAGE_DIR")
DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER")
RMP_RUST_BINARY = "rmp-experiment/target/release/batch"

def list_dta_files_from_dropbox(dbx):
    """
    Lists .dta files from the specified Dropbox folder.
    """
    try:
        path = f"/{DROPBOX_FOLDER}/data-files/"
        result = dbx.files_list_folder(path)
        dta_files = [
            entry
            for entry in result.entries
            if isinstance(entry, dropbox.files.FileMetadata) and entry.name.endswith(".dta")
        ]
        return dta_files
    except dropbox.exceptions.ApiError as err:
        print(f"*** Dropbox API error: {err}")
        return []

def download_from_dropbox(dbx, dropbox_path, local_path):
    """
    Downloads a file from Dropbox.
    """
    try:
        dbx.files_download_to_file(local_path, dropbox_path)
        print(f"  Downloaded {dropbox_path} to {local_path}")
    except dropbox.exceptions.ApiError as err:
        print(f"*** Dropbox API error: {err}")
        raise

def convert_dta_to_parquet(dta_path, parquet_path):
    """
    Converts a .dta file to a .parquet file with the required columns.
    """
    try:
        df = pd.read_stata(dta_path)
        # Assuming the .dta file has 'university' and 'fullname' columns
        df[['firstname', 'lastname']] = df['fullname'].str.split(' ', 1, expand=True)
        df_to_process = df[['university', 'firstname', 'lastname']]
        df_to_process.to_parquet(parquet_path)
        print(f"  Converted {dta_path} to {parquet_path}")
    except Exception as e:
        print(f"  Error converting {dta_path} to parquet: {e}")
        raise

def run_rmp_rust_binary(parquet_path):
    """
    Runs the RMP Rust binary to process the Parquet file.
    """
    try:
        if not os.path.exists(RMP_RUST_BINARY):
            print("  Rust binary not found, compiling...")
            subprocess.run(
                ["cargo", "build", "--release", "--manifest-path", "rmp-experiment/Cargo.toml"],
                check=True,
                capture_output=True,
                text=True,
            )
        print(f"  Running RMP Rust binary on {parquet_path}...")
        result = subprocess.run(
            [RMP_RUST_BINARY, parquet_path],
            check=True,
            capture_output=True,
            text=True,
        )
        print("  Rust binary output:")
        print(result.stdout)
        if result.stderr:
            print("  Rust binary error output:")
            print(result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"  Error running rust binary: {e}")
        print(e.stdout)
        print(e.stderr)
        raise

def merge_results_and_save_dta(original_dta_path, processed_parquet_path, output_dta_path):
    """
    Merges the processed data back into the original DataFrame and saves it as a new .dta file.
    """
    try:
        df_original = pd.read_stata(original_dta_path)
        df_processed = pd.read_parquet(processed_parquet_path)

        # Merge the department_rmp column
        df_merged = pd.merge(
            df_original,
            df_processed[['firstname', 'lastname', 'university', 'department_rmp']],
            on=['firstname', 'lastname', 'university'],
            how='left'
        )

        df_merged.to_stata(output_dta_path, write_index=False)
        print(f"  Saved processed data to {output_dta_path}")
    except Exception as e:
        print(f"  Error merging results and saving .dta file: {e}")
        raise

def main():
    """
    Main function to orchestrate the RMP data pipeline.
    """
    print("Starting RMP data pipeline...")

    # Authenticate with Dropbox
    dbx = dropbox_oauth()

    # List .dta files from Dropbox
    dta_files = list_dta_files_from_dropbox(dbx)
    if not dta_files:
        print("No .dta files found in Dropbox folder.")
        return

    print(f"Found {len(dta_files)} .dta files to process:")
    for file in dta_files:
        print(f"  - {file.name}")

    # Create a temporary directory for processing
    temp_dir = "temp_files"
    os.makedirs(temp_dir, exist_ok=True)

    for file in dta_files:
        print(f"Processing {file.name}...")

        # 1. Download
        local_dta_path = os.path.join(temp_dir, file.name)
        download_from_dropbox(dbx, file.path_lower, local_dta_path)

        # 2. Convert to parquet
        local_parquet_path = os.path.join(temp_dir, f"{os.path.splitext(file.name)[0]}.parquet")
        convert_dta_to_parquet(local_dta_path, local_parquet_path)

        # 3. Run rust binary
        run_rmp_rust_binary(local_parquet_path)

        # 4. Convert back to dta
        output_dta_path = os.path.join(temp_dir, f"{os.path.splitext(file.name)[0]}_processed.dta")
        merge_results_and_save_dta(local_dta_path, local_parquet_path, output_dta_path)

        # 5. Upload to dropbox
        # ... to be implemented

    print("RMP data pipeline finished.")

if __name__ == "__main__":
    main()
