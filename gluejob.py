import boto3
import jsonlines
import json
import os
import re
import time
from collections import defaultdict
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# AWS profile name
aws_profile = 'PowerUserAccess-484850288072'

# S3 client using specific profile
session = boto3.Session(profile_name=aws_profile)
s3 = session.client('s3')

# AWS S3 bucket and prefixes
source_bucket = 'gulp-data-vault-decode'
source_prefix = '4037-updated-10aug/'

destination_bucket = 'gulp-data-vault-decode'
destination_prefix = '4037-updated-10aug-merged/'

# Temporary directory to store downloaded files
local_temp_dir = r"D:\tmp"

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

# Function to handle S3 upload with retries and exponential backoff
def s3_upload_with_retry(bucket, key, data, retries=5, backoff_factor=0.5):
    for attempt in range(retries):
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=data)
            print(f"Successfully uploaded {key} to {bucket}")
            return
        except ClientError as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(backoff_factor * (2 ** attempt))
    print(f"Failed to upload {key} to {bucket} after multiple attempts.")

# Function to merge JSON objects from files with similar prefixes
def merge_json_files_locally(source_bucket, source_prefix, destination_bucket, destination_prefix):
    try:
        # Ensure the local temporary directory exists
        print(f"Checking if local temporary directory '{local_temp_dir}' exists...")
        if not os.path.exists(local_temp_dir):
            os.makedirs(local_temp_dir)
            print(f"Created local temporary directory: {local_temp_dir}")
        else:
            print(f"Local temporary directory already exists: {local_temp_dir}")

        # Get the count of files per prefix
        prefix_count = count_files_by_prefix(source_bucket, source_prefix)

        print(prefix_count)

        # Dictionary to track the number of processed files for each prefix
        processed_files_count = defaultdict(int)

        # Pagination handling for listing objects
        paginator = s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': source_bucket, 'Prefix': source_prefix}
        page_iterator = paginator.paginate(**operation_parameters)

        print("Starting the merging of JSON files locally...")
        file_counter = 0

        for page in page_iterator:
            print("Processing a new page of objects...")
            for obj in page.get('Contents', []):
                source_key = obj['Key']
                print(f"Processing file: {source_key}")

                # Process files ending with the specific years
                if source_key.endswith(".json") and re.search(r'_(20[0-2][0-9]|19[9-9][0-9])', source_key):
                    # Safe filename handling
                    file_name = re.sub(r'[^\w\-_\. ]', '_', os.path.basename(source_key))
                    local_file_path = os.path.join(local_temp_dir, file_name)
                    print(f"Local file path for download: {local_file_path}")

                    # Download the file from S3
                    s3.download_file(source_bucket, source_key, local_file_path)
                    print(f"Downloaded file: {local_file_path}")

                    # Extract the prefix before the year part
                    prefix = re.split(r'_(20[0-2][0-9]|19[9-9][0-9])', file_name)[0]
                    print(f"Extracted prefix: {prefix}")

                    # Read the JSON lines file and merge JSON objects
                    print(f"Reading and merging JSON objects from file: {local_file_path}")
                    with jsonlines.open(local_file_path) as reader:
                        with open(f"{local_temp_dir}/{prefix}_merged.json", 'a') as temp_file:
                            for json_obj in reader:
                                temp_file.write(json.dumps(json_obj) + "\n")
                    print(f"Merged data for prefix: {prefix}")

                    # Delete file from local temp directory
                    os.remove(local_file_path)
                    print(f"Deleted local file: {local_file_path}")

                    # Increment the processed files count for this prefix
                    processed_files_count[prefix] += 1
                    file_counter += 1
                    print(f"Processed file {file_counter}/{sum(prefix_count.values())} for prefix {prefix}")

                    # If all files for this prefix are processed, upload the merged data to S3
                    if processed_files_count[prefix] == prefix_count[prefix]:
                        print(f"All files for prefix '{prefix}' processed. Uploading merged data to S3...")
                        s3_key = f"{destination_prefix}{prefix}.json"
                        with open(f"{local_temp_dir}/{prefix}_merged.json", 'rb') as temp_file:
                            s3_upload_with_retry(destination_bucket, s3_key, temp_file)
                        print(f"Uploaded merged file for prefix: {s3_key}")
                        os.remove(f"{local_temp_dir}/{prefix}_merged.json")  # Clear the temporary file
                        print(f"Deleted local merged file: {local_temp_dir}/{prefix}_merged.json")

        print("Merging and uploading process completed successfully.")
    except NoCredentialsError:
        print("Error: AWS credentials not found.")
    except PartialCredentialsError:
        print("Error: Incomplete AWS credentials provided.")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Call the function to merge JSON files locally and upload to S3
merge_json_files_locally(source_bucket, source_prefix, destination_bucket, destination_prefix)
