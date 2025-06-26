import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from io import StringIO
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

S3_BUCKET_NAME = "cric-info-raw"
TABLES_LIST = [
    "PLAYER_INFO",
    "PLAYER_STATS",
    "MATCH_INFO",
    "SERIES_INFO",
]


def get_secret(secret_name: str, region_name: str = "ap-south-1"):
    """
    Fetches a secret from AWS Secrets Manager.
    :param secret_name: Name of the secret
    :param region_name: AWS region where the secret is stored
    :return: Secret value as a dictionary
    """
    try:
        client = boto3.client("secretsmanager", region_name=region_name)
        logger.info(f"Fetching secret: {secret_name}")
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except Exception as e:
        logger.error(f"An error occurred while fetching the secret: {e}")
        raise e


def generate_timestamp_format():
    """
    this method is used to generate the timestamp format
    :return: timestamp_format
    """
    return datetime.now().strftime("%Y%m%d%H%M%S")


def generate_file_name(file_name: str, file_type: str) -> str:
    """
    this method is used to generate the file_name
    :param file_name: name of the file
    :param file_type: type of the file (e.g., json, csv)
    :return: complete file name with timestamp and iteration
    """
    return f"snowflake-data/{file_name}/{generate_timestamp_format()}_{file_name}.{file_type}"


def create_sf_connection(conn_params: dict):
    """
    Creates a Snowflake connection using the provided secret.
    :param conn_params: Dictionary containing Snowflake credentials
    :return: Snowflake connection object
    """
    try:
        conn = snowflake.connector.connect(
            user=conn_params["username"],
            password=conn_params["password"],
            account=conn_params["account"],
            warehouse=conn_params["warehouse"],
            database=conn_params["database"],
            schema=conn_params["schema"],
            role=conn_params.get("role", None),
        )
        logger.info("started a Snowflake connection")
        return conn
    except snowflake.connector.errors.Error as er:
        logger.error(f"An error occurred while creating the Snowflake connection: {er}")
        raise er


def export_data_to_s3(conn: SnowflakeConnection, tables_list: list):
    """
    Exports data from specified Snowflake tables to S3 in CSV format.
    :param conn: Snowflake connection object
    :param tables_list: List of table names to export
    """
    for table in tables_list:
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, conn)
        logger.info(f"Exporting data from table: {table} with {len(df)} records")
        s3 = boto3.client("s3")
        key = generate_file_name(table, "csv")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())
        logger.info(f"Data exported to S3 bucket {S3_BUCKET_NAME} with key {key}")


if __name__ == "__main__":
    try:
        secret = get_secret("snowflake/creds", "ap-south-1")
        connection = create_sf_connection(secret)
        export_data_to_s3(connection, TABLES_LIST)
        connection.close()
        logger.info("Snowflake connection closed")
        logger.info("Data export completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
