from ayx import Alteryx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import tempfile
import os

# Initialize S3 client
s3_client = boto3.client('s3')

# Function to upload a file to S3
def upload_to_s3(local_file, bucket_name, output_prefix, output_file_name):
    output_file_key = f"{output_prefix}{output_file_name}"
    s3_client.upload_file(local_file, bucket_name, output_file_key)
    print(f"Parquet file uploaded to S3: {output_file_key}")

# Convert Alteryx DataFrame to Parquet and upload to S3
def alteryx_to_parquet_s3(df, bucket_name, output_prefix, output_file_name):
    # Convert DataFrame to Arrow Table
    table = pa.Table.from_pandas(df)

    # Write to a temporary Parquet file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        pq.write_table(table, temp_file.name)

        # Upload the temporary file to S3
        upload_to_s3(temp_file.name, bucket_name, output_prefix, output_file_name)

    # Clean up temporary file
    os.unlink(temp_file.name)

# Main function to load data from Alteryx and handle conversion
def main():
    # Replace with your bucket and prefix
    bucket_name = 'your-bucket-name'
    output_prefix = 'your-output-prefix/'
    output_file_name = 'alteryx_output.parquet'

    # Load data from Alteryx input
    df = Alteryx.read('#1')  # Loads the data from the workflow

    # Convert to Parquet and upload to S3
    alteryx_to_parquet_s3(df, bucket_name, output_prefix, output_file_name)

if __name__ == "__main__":
    main()
