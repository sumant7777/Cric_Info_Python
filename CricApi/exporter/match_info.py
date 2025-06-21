import requests
from util import generate_file_name, write_to_json
from s3 import upload_file_s3
from load_env import get_env_vars
from util import logger
from snow import create_session, execute_query, get_count
from s3 import s3_archiver

config = get_env_vars()


def get_match_info(api_config, offset):
    """
    This method is used to get the matches list from the Api
    :param api_config: config
    :param offset: offset
    :return: matches info in json format
    """
    BATCH = 25
    iteration = 0
    while True:
        try:
            url = f"{api_config.MATCHES_LIST_API_URL}apikey={api_config.API_KEY}&offset={offset}"
            response = requests.get(url)
            if response.status_code == 200:
                response_json = response.json().get("data", [])
                if not response_json:
                    logger.info("No more data returned.")
                    break
                offset += len(response_json)
                iteration += 1
                file_name = generate_file_name("matches", "json", iteration)
                write_to_json(response_json, file_name)
                logger.info(f"Saved batch {iteration} to {file_name}")
                upload_file_s3(
                    file_name, config.MATCH_INFO_S3_DEST, config.CRIC_INFO_BUCKET
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


def execute_match_info(match_config) -> str:
    """
    This method is used to execute the matches info retrieval.
    :param match_config: tuple with api config
    :return: status message
    """
    count = get_count(match_config.SF_DATABASE, "cric_data", "player_info", "match_id")
    logger.info("Starting to get matches info")
    get_match_info(match_config, count)
    logger.info("Completed getting players info")
    execute_query(create_session(), rf"{config.SQL_FILE_PATH}" + "match_info.sql")
    logger.info("Player stats loaded into Snowflake")
    s3_archiver(
        config.MATCH_INFO_S3_DEST, config.CRIC_INFO_BUCKET, config.CRIC_INFO_BUCKET
    )
    logger.info("Player stats archived in S3")
    return "Player stats extraction and loading completed successfully."


if __name__ == "__main__":
    result = execute_match_info(config)
    logger.info(result)
