import boto3
import os
from urllib.parse import urlparse
from botocore import UNSIGNED
from botocore.client import Config

def download_single_file(s3_url, local_file_path):
    # Parse the S3 URL to get the bucket name and object key
    parsed_url = urlparse(s3_url)
    bucket_name = parsed_url.netloc
    object_key = parsed_url.path.lstrip('/')
    
    # Create the S3 client with unsigned config for public access
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    try:
        # Download the file
        print(f"Downloading {object_key} from bucket {bucket_name} to {local_file_path}")
        s3.download_file(bucket_name, object_key, local_file_path)
        print(f"Download complete: {local_file_path}")
    except s3.exceptions.NoSuchBucket:
        print(f"Bucket {bucket_name} does not exist.")
    except s3.exceptions.NoSuchKey:
        print(f"Object {object_key} does not exist.")
    except Exception as e:
        print(f"Error downloading file: {e}")



def dump_bucket(bucket_url, local_folder):
    # Parse the S3 URL to get the bucket name
    parsed_url = urlparse(bucket_url)
    bucket_name = parsed_url.netloc

    # Create the S3 client with unsigned config for public access
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    try:
        # List all objects in the bucket
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    object_key = obj['Key']
                    # Define the local file path where you want to save the file
                    local_file_path = os.path.join(local_folder, object_key)
                    
                    # Ensure the local folder exists
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    
                    # Download the file
                    print(f"Downloading {object_key} from bucket {bucket_name} to {local_file_path}")
                    s3.download_file(bucket_name, object_key, local_file_path)
                    print(f"Download complete: {local_file_path}")
    except Exception as e:
        print(f"Error dumping bucket: {e}")





if __name__ == "__main__":
    # User choice for downloading file or dumping bucket
    print("Download file from S3 or dump bucket:")
    print("1. Download single file")
    print("2. Dump entire bucket")
    user_input = input("Enter your choice (1 or 2): ")
    
    if user_input == "1":
        s3_url = input("Enter the S3 file URI: ")
    
        # Define the local file path where you want to save the file
        cwd = os.getcwd()
        local_file_path = os.path.join(cwd, 'download', os.path.basename(urlparse(s3_url).path))
        
        # Ensure the local folder exists
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        # Download the file
        download_single_file(s3_url, local_file_path)
    
    elif user_input == "2":
        bucket_url = input("Enter the S3 bucket URI (e.g., s3://your-bucket-name): ")
        
        # Define the local folder where you want to save all files
        cwd = os.getcwd()
        local_folder = os.path.join(cwd, 'download')
        
        # Ensure the local folder exists
        os.makedirs(local_folder, exist_ok=True)
        
        # Dump the entire bucket
        dump_bucket(bucket_url, local_folder)
    
    else:
        print("Invalid choice. Please enter 1 or 2.")
