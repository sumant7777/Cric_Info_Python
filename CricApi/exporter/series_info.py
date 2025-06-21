import requests
from util import generate_file_name, write_to_json
from s3 import upload_file_s3, s3_archiver
from load_env import get_env_vars
from util import logger
from snow import create_session, execute_query, get_count


def get_series_info(api_config, offset):
    """
    This method is used to get the series list from the Api
    :param api_config: config
    :param offset: offset
    :return: list of series in json format
    """
    BATCH = 25
    iteration = 0
    while True:
        try:
            url = f"{api_config.SERIES_LIST_API_URL}apikey={api_config.API_KEY}&offset={offset}"
            response = requests.get(url)
            if response.status_code == 200:
                response_json = response.json().get("data", [])
                if not response_json:
                    logger.info("No more data returned.")
                    break
                offset += len(response_json)
                iteration += 1
                file_name = generate_file_name("series", "json", iteration)
                write_to_json(response_json, file_name)
                logger.info(f"Saved batch {iteration} to {file_name}")
                upload_file_s3(
                    file_name, config.SERIES_INFO_S3_DEST, config.CRIC_INFO_BUCKET
                )
                if len(response_json) < BATCH:
                    logger.info("Final batch received. Ending loop.")
                    break
            else:
                logger.exception(
                    f"Request failed with status code {response.status_code}: {url}"
                )
                break
        except Exception as e:
            logger.error(f"An exception occurred; {e}")
            raise e


def execute_series_info(series_config) -> str:
    """
    This method is used to execute the matches info retrieval.
    :param series_config: tuple with api config
    :return: status message
    """
    logger.info("Starting to get matches info")
    count = get_count(
        series_config.SF_DATABASE, "cric_data", "player_info", "SERIES_ID"
    )
    get_series_info(series_config, count)
    logger.info("Completed getting players info")
    execute_query(create_session(), rf"{config.SQL_FILE_PATH}" + "series_info.sql")
    logger.info("Player stats loaded into Snowflake")
    s3_archiver(
        config.SERIES_INFO_S3_DEST, config.CRIC_INFO_BUCKET, config.CRIC_INFO_BUCKET
    )
    logger.info("Player stats archived in S3")
    return "Player stats extraction and loading completed successfully."


if __name__ == "__main__":
    config = get_env_vars()
    result = execute_series_info(config)
    logger.info(result)
