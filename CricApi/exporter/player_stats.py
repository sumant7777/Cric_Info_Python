import requests
from util import generate_file_name, write_to_json
from load_env import get_env_vars
from s3 import upload_file_s3
from util import logger
from snow import create_session, execute_query, get_id
from s3 import s3_archiver


def get_player_stats(api_config: tuple, id_list: list) -> str:
    """
    this method is used to get the players stats from the Api
    :param api_config: tuple with api config
    :param id_list: list of id's
    :return: json with players stats
    """
    OFFSET = 0
    BATCH = 25
    iteration = 0
    for player_id in id_list:
        while True:
            try:
                url = (
                    f"{api_config.PLAYER_INFO_API_URL}apikey={api_config.API_KEY}"
                    f"&id={player_id}&offset={OFFSET}"
                )
                response = requests.get(url)
                if response.status_code == 200:
                    response_json = response.json().get("data", [])
                    if not response_json:
                        logger.info(
                            f"No more data returned for players ID {player_id}."
                        )
                        break

                    OFFSET += len(response_json)
                    iteration += 1
                    file_name = generate_file_name("player_stats", "json", iteration)
                    write_to_json(response_json, file_name)
                    upload_file_s3(
                        file_name,
                        api_config.PLAYER_STATS_S3_DEST,
                        api_config.CRIC_INFO_BUCKET,
                    )
                    logger.info(f"Saved batch {iteration} to {file_name}")
                    if len(response_json) < BATCH:
                        logger.info("Final batch received")
                        break
                else:
                    logger.info(
                        f"Request failed with status code {response.status_code}: {url}"
                    )
                    break
            except Exception as e:
                logger.exception(
                    f"An exception occurred for players ID {player_id}: {e}"
                )
                break
    return "Export of stats for players completed successfully."


def execute_player_stats(player_config: tuple) -> str:
    """
    This method is used to execute the players info retrieval.
    :param player_config: tuple with api config
    :return: status message
    """
    player_id_list = get_id(config.SF_DATABASE, "cric_data", "player_info", "PLAYER_ID")
    player_stats_id_list = get_id(
        config.SF_DATABASE, "cric_data", "player_stats", "PLAYER_ID"
    )
    player_id_list = list(set(player_id_list) - set(player_stats_id_list))
    player_id_list = player_id_list[:100]
    logger.info("Starting to get players info")
    get_player_stats(player_config, player_id_list)
    logger.info("Completed getting players info")
    execute_query(create_session(), rf"{config.SQL_FILE_PATH}" + "player_stats.sql")
    logger.info("Player stats loaded into Snowflake")
    s3_archiver(
        config.PLAYER_STATS_S3_DEST, config.CRIC_INFO_BUCKET, config.CRIC_INFO_BUCKET
    )
    logger.info("Player stats archived in S3")
    return "Player stats extraction and loading completed successfully."


if __name__ == "__main__":
    config = get_env_vars()
    result = execute_player_stats(config)
    logger.info(config)
