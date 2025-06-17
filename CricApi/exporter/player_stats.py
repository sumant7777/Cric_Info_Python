import requests
from lib import generate_file_name, write_to_json
from load_env import get_env_vars
from s3 import upload_file_s3
from get_id import get_id
from lib import logger


config = get_env_vars()
player_id_list = get_id(config.SF_DATABASE, "cric_data", "player_info", "PLAYER_ID")
player_stats_id_list = get_id(config.SF_DATABASE, "cric_data", "player_stats", "PLAYER_ID")

player_id_list = list(set(player_id_list) - set(player_stats_id_list))
player_id_list = player_id_list[:100]


def get_player_info(api_config:tuple, id_list:list) -> str:
    """
    this method is used to get the player stats from the Api
    :param api_config: tuple with api config
    :param id_list: list of id's
    :return: json with player stats
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
                        logger.info(f"No more data returned for player ID {player_id}.")
                        break

                    OFFSET += len(response_json)
                    iteration += 1
                    file_name = generate_file_name("player_stats", "json", iteration)
                    write_to_json(response_json, file_name)
                    upload_file_s3(
                        file_name,
                        api_config.CRIC_INFO_BUCKET,
                        api_config.PLAYER_STATS_S3_DEST,
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
                logger.exception(f"An exception occurred for player ID {player_id}: {e}")
                break
    return "Export of stats for players completed successfully."


get_player_info(config, player_id_list)
