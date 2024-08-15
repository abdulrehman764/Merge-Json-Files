import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import jsonlines
import json
import re
import os
from collections import defaultdict
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

# Initialize GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# S3 client
s3 = boto3.client('s3')

# S3 bucket and prefixes
source_bucket = 'gulp-data-vault-decode'
source_prefix = '4037-updated-10aug-test/'
destination_bucket = 'gulp-data-vault-decode'
destination_prefix = '4037-updated-10aug-merged-test/'

# Function to count files per prefix
def count_files_by_prefix(source_bucket, source_prefix):
    print(f"Counting files per prefix in bucket '{source_bucket}' with prefix '{source_prefix}'...")
    prefix_count = defaultdict(int)
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': source_bucket, 'Prefix': source_prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    
    for page in page_iterator:
        for obj in page.get('Contents', []):
            source_key = obj['Key']
            if source_key.endswith(".json") and re.search(r'_(20[0-2][0-9]|19[9-9][0-9])', source_key):
                file_name = os.path.basename(source_key)
                prefix = re.split(r'_(20[0-2][0-9]|19[9-9][0-9])', file_name)[0]
                prefix_count[prefix] += 1
    
    print(f"File counts by prefix: {dict(prefix_count)}")
    return prefix_count

# Function to merge JSON objects from files with similar prefixes
def merge_json_files(source_bucket, source_prefix, destination_bucket, destination_prefix):
    try:
        prefix_count = count_files_by_prefix(source_bucket, source_prefix)

        processed_files_count = defaultdict(int)
        paginator = s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': source_bucket, 'Prefix': source_prefix}
        page_iterator = paginator.paginate(**operation_parameters)

        print("Starting the merging of JSON files...")

        for page in page_iterator:
            for obj in page.get('Contents', []):
                source_key = obj['Key']
                print(f"Processing file: {source_key}")

                if source_key.endswith(".json") and re.search(r'_(20[0-2][0-9]|19[9-9][0-9])', source_key):
                    file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(source_key))
                    local_file_path = f"/tmp/{file_name}"
                    s3.download_file(source_bucket, source_key, local_file_path)
                    print(f"Downloaded file: {local_file_path}")

                    prefix = re.split(r'_(20[0-2][0-9]|19[9-9][0-9])', file_name)[0]

                    with jsonlines.open(local_file_path) as reader:
                        merged_data = [json_obj for json_obj in reader]
                    merged_json = '\n'.join(json.dumps(record) for record in merged_data)

                    if processed_files_count[prefix] == prefix_count[prefix] - 1:
                        s3_key = f"{destination_prefix}{prefix}.json"
                        s3.put_object(Bucket=destination_bucket, Key=s3_key, Body=merged_json)
                        print(f"Uploaded merged file for prefix: {s3_key}")
                    else:
                        processed_files_count[prefix] += 1

                    os.remove(local_file_path)

        print("Merging and uploading process completed successfully.")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
    except ClientError as e:
        print(f"Client error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Call the function to merge JSON files and upload to S3
merge_json_files(source_bucket, source_prefix, destination_bucket, destination_prefix)

# Complete the job
job.commit()
