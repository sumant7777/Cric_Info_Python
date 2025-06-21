import requests
from datetime import timedelta
from util import generate_file_name, write_to_json
from load_env import get_env_vars

from util import logger
from prefect import task, flow
from prefect.tasks import task_input_hash
from s3 import upload_file_s3, s3_archiver, check_file_availability
from snow import create_session, execute_query, get_count


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_player_info(api_config: tuple, offset: int):
    """
    This method is used to get the players list from the Api
    :param api_config:  config
    :param offset:  offset to start from
    :return: players info in json format
    """
    BATCH = 25
    iteration = 0
    while True:
        try:
            url = f"{api_config.PLAYER_LIST_API_URL}apikey={api_config.API_KEY}&offset={offset}"
            response = requests.get(url)
            if response.status_code == 200:
                response_json = response.json().get("data", [])
                if not response_json:
                    logger.info("No more data returned.")
                    break
                offset += len(response_json)
                iteration += 1
                file_name = generate_file_name("players", "json", iteration)
                write_to_json(response_json, file_name)
                upload_file_s3(
                    file_name,
                    api_config.PLAYER_INFO_S3_DEST,
                    api_config.CRIC_INFO_BUCKET,
                )
                logger.info(f"Saved batch {iteration} to {file_name}")
                if len(response_json) < BATCH:
                    logger.info("Final batch received. Ending loop.")
                    break
            else:
                logger.exception(
                    f"Request failed with status code {response.status_code}: {url}"
                )
                break
        except Exception as e:
            logger.error(f"An exception occurred : {e}")
            raise e
    return "Successfully completed the players list extraction from Api"


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def load_to_snowflake(file_name: str):
    """
    This method is used to load the players list from the Api
    :param file_name: file_name
    :return: status message
    """
    execute_query(create_session(), rf"{config.SQL_FILE_PATH}{file_name}")
    return "Player info loaded into Snowflake"


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def archive_files(player_info_config):
    """
    This method is used to archive the processed files in s3
    :param player_info_config: player_info_config
    :return: status message
    """
    s3_archiver(
        player_info_config.PLAYER_INFO_S3_DEST,
        player_info_config.CRIC_INFO_BUCKET,
        player_info_config.CRIC_INFO_BUCKET,
    )
    return logger.info("Player info files archived in S3")


@flow
def execute_player_info(player_config) -> str:
    """
    This method is used to execute the matches info retrieval.
    :param player_config: tuple with api config
    :return: status message
    """
    count = get_count(
        player_config.SF_DATABASE, "cric_data", "player_info", "player_id"
    )
    get_player_info(player_config, count)
    execute = check_file_availability(
        player_config.PLAYER_INFO_S3_DEST, player_config.CRIC_INFO_BUCKET
    )
    if execute:
        load_to_snowflake("player_info.sql")
        archive_files(player_config)
    return "Player info extraction and loading completed successfully."


if __name__ == "__main__":
    config = get_env_vars()
    result = execute_player_info(config)
    logger.info(result)
