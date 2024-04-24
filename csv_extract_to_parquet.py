import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_client = boto3.client('s3')

## Get the list of files in the csv_landing bucket
response = s3_client.list_objects_v2(Bucket='csv_landing')
files = response['Contents']

## Find the most recent CSV file
most_recent_file = max(files, key=lambda x: x['LastModified'])

## Read the most recent CSV file
df = spark.read.format("csv").option("header", "true").load("s3://csv_landing/" + most_recent_file['Key'])

## Read JSON schema from S3
schema_json = spark.read.json("s3://csv_landing/schema/file.json")

## Convert schema to Spark StructType
schema = spark.read.json(schema_json.toJSON()).schema

## Write Parquet file to S3 in the API_Ready folder
df.write.format("parquet").mode("overwrite").save("s3://csv_landing/API_Ready/")

## Trigger Lambda function
lambda_client = boto3.client('lambda')
response = lambda_client.invoke(
    FunctionName='your_lambda_function_arn_here',
    InvocationType='Event',
    Payload=json.dumps({'file_path': 's3://csv_landing/API_Ready/'})
)

job.commit()
