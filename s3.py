from dotenv import load_dotenv
import boto3
import os

load_dotenv()

def upload_file_s3(local_path,s3_bucket_name,s3_destination):
    s3 = boto3.client('s3')
    file_name= local_path.split('/')[-1]
    s3_destination = s3_destination+file_name
    s3.upload_file(local_path,s3_bucket_name,s3_destination)
    print(f"exported file {local_path} to {s3_destination}")
    os.remove(local_path)
    print(f"deleted file {local_path}")

