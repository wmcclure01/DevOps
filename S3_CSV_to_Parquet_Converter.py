import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import tempfile
import os
import json

s3_client = boto3.client('s3')

# Grabs the latest CSV file from S3
def get_latest_csv(bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        latest_csv = max(response['Contents'], key=lambda x: x['LastModified'])
        return latest_csv['Key']
    else:
        return None

# Grabs the latest schema file from S3
def get_latest_schema(bucket_name, prefix, file_key):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        schema_file = [obj for obj in response['Contents'] if obj['Key'] == file_key]
        if schema_file:
            latest_schema = max(schema_file, key=lambda x: x['LastModified'])
            return latest_schema['Key']
    return None

# Reads the CSV file from S3 and returns a Pandas DataFrame.
def read_csv_from_s3(bucket_name, file_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    csv_data = response['Body'].read()
    df = pd.read_csv(BytesIO(csv_data))

    # Convert 'd_strt' column to datetime if it contains date-like strings ##might remove
    df['d_strt'] = pd.to_datetime(df['d_strt'])
    return df

# Reads the schema file from S3 and returns a list of column names and data types.
def read_schema_from_json(bucket_name, schema_file_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=schema_file_key)
    schema_data = response['Body'].read().decode('utf-8')  # Read schema file content
    schema_json = json.loads(schema_data)  # Parse JSON data
    target_schema = [(col['name'], col['type']) for col in schema_json['columns']]
    return target_schema

# Converts the Pandas DataFrame to a Parquet file and uploads it to S3.
def convert_to_parquet_and_upload(df, bucket_name, output_prefix, file_key, target_schema):
  #other data types can be manually mapped as needed in variations  
  hive_to_arrow_type = {
        'int': pa.int64(),
        'string': pa.string(),
        'date': pa.date32()
    }
    arrow_schema = pa.schema([(col, hive_to_arrow_type[dtype]) for col, dtype in target_schema])

    table = pa.Table.from_pandas(df, schema=arrow_schema)

    # Write Parquet file to a local temporary file
    local_file = tempfile.NamedTemporaryFile(delete=False)
    pq.write_table(table, local_file.name)

    # Upload the local Parquet file to S3
    output_file_key = f"{output_prefix}{file_key.split('/')[-1].split('.')[0]}.parquet"
    s3_client.upload_file(local_file.name, bucket_name, output_file_key)

    # Remove the temporary local file
    os.unlink(local_file.name)
    print(f"Parquet file uploaded to S3: {output_file_key}")
    return output_file_key

#Main function that defines bucket_name, input_prefix, schema_prefix, and output_prefix.
def main():
    bucket_name = 'your-bucket-name'
    input_prefix = 'your-input-prefix/'
    schema_prefix = 'your-schema-prefix/'  # Prefix for schema files
    output_prefix = 'your-output-prefix/'

    latest_csv_key = get_latest_csv(bucket_name, input_prefix)
    if latest_csv_key:
        df = read_csv_from_s3(bucket_name, latest_csv_key)
       
        # Get the corresponding schema file for the latest CSV file
        schema_file_key = latest_csv_key.replace(input_prefix, schema_prefix).replace('.csv', '.json')
        latest_schema_key = get_latest_schema(bucket_name, schema_prefix, schema_file_key)
        if latest_schema_key:
            target_schema = read_schema_from_json(bucket_name, latest_schema_key)
           
            # Convert CSV to Parquet using the fetched target schema
            convert_to_parquet_and_upload(df, bucket_name, output_prefix, latest_csv_key, target_schema)


if __name__ == "__main__":
    main()
