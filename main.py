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


#dumps entire bucket contents to download folder
def dump_bucket():
    pass


if __name__ == "__main__":
    # User-provided S3 URL
    s3_url = input("Enter the S3 URI: ")
    
    # Define the local file path where you want to save the file
    cwd = os.getcwd()
    local_file_path = os.path.join(cwd, 'download', os.path.basename(urlparse(s3_url).path))
    
    # Ensure the local folder exists
    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
    
    # Download the file
    download_single_file(s3_url, local_file_path)
