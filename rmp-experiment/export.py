import pandas as pd
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from stata_conversion import _convert_boolean_columns, _convert_float_columns, _process_string_columns

def add_rmp_column_to_stata_file(stata_file_path, rmp_parquet_file_path):
    df = pd.read_stata(stata_file_path)
    df_rmp = pd.read_parquet(rmp_parquet_file_path)

    # Getting clean rmp data
    df_rmp = df_rmp[['firstname', 'lastname', 'university', 'department_rmp']]
    df_rmp = df_rmp.drop_duplicates()
    df_rmp = df_rmp.dropna(subset=['department_rmp'])
    print(f"Number of rows in rmp data: {len(df_rmp)}")
    
    # Merging with main df
    df = df.merge(
        df_rmp[['firstname', 'lastname', 'university', 'department_rmp']], 
        on=['firstname', 'lastname', 'university'], 
        how='left'
    )
    print(f"Number of rows in merged df: {len(df[df['department_rmp'].notna()])}")

    # Saving to stata file
    df_stata = df.copy()
    
    df_stata = _convert_boolean_columns(df_stata)
    # df_stata = _convert_float_columns(df_stata)
    df_stata = _process_string_columns(df_stata)

    df.to_stata(stata_file_path, version=118, write_index=False)

    print(f'{stata_file_path} updated with rmp data')

    
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python export.py <stata_file_path> <rmp_parquet_file_path>")
        sys.exit(1)

    stata_file_path = sys.argv[1]
    rmp_parquet_file_path = sys.argv[2]
    add_rmp_column_to_stata_file(stata_file_path, rmp_parquet_file_path)