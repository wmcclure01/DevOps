# S3 CSV to Parquet Converter

This project contains a Python script to automate the process of converting CSV files stored in an S3 bucket to Parquet format using a predefined schema. The converted Parquet files are then uploaded back to the same S3 bucket.

## Prerequisites

- Python 3.x
- Boto3
- Pandas
- PyArrow

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/your-username/s3-csv-to-parquet.git
    cd s3-csv-to-parquet
    ```

2. Create a virtual environment and activate it:
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3. Install the required packages:
    ```bash
    pip install boto3 pandas pyarrow
    ```

## Configuration

Ensure you have AWS credentials configured. You can set up the credentials using the AWS CLI:
```bash
aws configure

or by setting the environment variables:

export AWS_ACCESS_KEY_ID='your-access-key-id'
export AWS_SECRET_ACCESS_KEY='your-secret-access-key'
export AWS_DEFAULT_REGION='your-region'

Usage
Update the script with the appropriate S3 bucket names and prefixes:

python
Copy code
bucket_name = 'your-bucket-name'
input_prefix = 'your-input-prefix/'
schema_prefix = 'your-schema-prefix/'  # Prefix for schema files
output_prefix = 'your-output-prefix/'
Run the script:

bash
Copy code
python s3_csv_to_parquet.py
Script Details
The script performs the following tasks:
