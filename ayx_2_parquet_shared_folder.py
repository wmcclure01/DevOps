from ayx import Alteryx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Function to convert DataFrame to Parquet and save to a shared folder
def alteryx_to_parquet_shared_folder(df, output_folder, output_file_name):
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Full path to the output file
    output_file_path = os.path.join(output_folder, output_file_name)

    # Convert DataFrame to Arrow Table and write to Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file_path)

    print(f"Parquet file saved to: {output_file_path}")

# Main function to load data from Alteryx and handle conversion
def main():
    # Replace with your shared folder path and file name
    output_folder = '/shared/network/path/'
    output_file_name = 'alteryx_output.parquet'

    # Load data from Alteryx input
    df = Alteryx.read('#1')  # Loads the data from the workflow

    # Convert to Parquet and save to the shared folder
    alteryx_to_parquet_shared_folder(df, output_folder, output_file_name)

if __name__ == "__main__":
    main()
