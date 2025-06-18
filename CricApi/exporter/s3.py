from dotenv import load_dotenv
import boto3
from lib import logger
from datetime import datetime
from load_env import get_env_vars

load_dotenv()
config = get_env_vars()

s3 = boto3.client("s3")


def upload_file_s3(
    local_path: str, s3_destination: str, s3_bucket_name: str = config.CRIC_INFO_BUCKET
):
    """
    Uploads a file to an S3 bucket.
    :param local_path: Path to the local file to be uploaded.
    :param s3_destination: Destination path in the S3 bucket.
    :param s3_bucket_name: Name of the S3 bucket.
    """
    try:
        file_name = local_path.split("/")[-1]
        s3_destination = s3_destination + file_name
        s3.upload_file(local_path, s3_bucket_name, s3_destination)
        logger.info(f"exported file {local_path} to {s3_destination}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e


def download_file_s3(
    s3_key: str, local_path: str, s3_bucket_name: str = config.CRIC_INFO_BUCKET
):
    """
    Downloads a file from an S3 bucket.
    :param s3_bucket_name: Name of the S3 bucket.
    :param s3_key: Source path in the S3 bucket.
    :param local_path: Local path where the file will be saved.
    """
    try:
        s3.download_file(s3_bucket_name, s3_key, local_path)
        logger.info(f"Downloaded file from {s3_key} to {local_path}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e


def list_files_s3(s3_bucket_name: str = config.CRIC_INFO_BUCKET, prefix: str = ""):
    """
    Lists files in an S3 bucket with a specific prefix.
    :param s3_bucket_name: Name of the S3 bucket.
    :param prefix: Prefix to filter files in the bucket.
    :return: List of files in the bucket with the specified prefix.
    """
    try:
        response = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=prefix)
        if "Contents" in response:
            files = [obj["Key"] for obj in response["Contents"]]
            logger.info(
                f"Files in bucket {s3_bucket_name} with prefix '{prefix}': {files}"
            )
            return files
        else:
            logger.info(
                f"No files found in bucket {s3_bucket_name} with prefix '{prefix}'"
            )
            return []
    except Exception as e:
        logger.error(f"An error occurred while listing files: {e}")
        raise e


def delete_file_s3(s3_key: str, s3_bucket_name: str = config.CRIC_INFO_BUCKET):
    """
    Deletes a file from an S3 bucket.
    :param s3_key: Path of the file in the S3 bucket to be deleted.
    :param s3_bucket_name: Name of the S3 bucket.
    """
    try:
        s3.delete_object(Bucket=s3_bucket_name, Key=s3_key)
        logger.info(f"Deleted file {s3_key} from bucket {s3_bucket_name}")
    except Exception as e:
        logger.error(f"An error occurred while deleting file: {e}")
        raise e


def copy_file_s3(
    source_key: str,
    dest_key: str,
    source_bucket: str = config.CRIC_INFO_BUCKET,
    dest_bucket: str = config.CRIC_INFO_BUCKET,
):
    """
    Copies a file from one S3 bucket to another.
    :param source_key: Key of the file in the source bucket.
    :param dest_key: Key for the copied file in the destination bucket.
    :param source_bucket: Name of the source S3 bucket.
    :param dest_bucket: Name of the destination S3 bucket.
    """
    try:
        copy_source = {"Bucket": source_bucket, "Key": source_key}
        s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
        logger.info(
            f"Copied file from {source_bucket}/{source_key} to {dest_bucket}/{dest_key}"
        )
    except Exception as e:
        logger.error(f"An error occurred while copying file: {e}")
        raise e


def s3_archiver(
    prefix: str,
    src_bucket_name: str = config.CRIC_INFO_BUCKET,
    dest_bucket_name: str = config.CRIC_INFO_BUCKET,
):
    """
    Archives files from one S3 bucket to another with a timestamped folder structure.
    :param src_bucket_name: Name of the source S3 bucket.
    :param dest_bucket_name: Name of the destination S3 bucket.
    :param prefix: Prefix to filter files in the source bucket.
    """
    try:
        timestamp_folder = datetime.now().strftime("%Y-%m-%d")
        s3_keys = list_files_s3(src_bucket_name, prefix)
        for key in s3_keys:
            copy_file_s3(
                key,
                f"archive/{timestamp_folder}/" + key,
                src_bucket_name,
                dest_bucket_name,
            )
            s3_archive_destination = f"archived/{timestamp_folder}" + key
            delete_file_s3(src_bucket_name, key)
            logger.info(f"Deleted {key} from {src_bucket_name}")
            logger.info(f"Archived {key} to {s3_archive_destination}")
    except Exception as e:
        logger.error(f"An error occurred during archiving: {e}")
        raise e
