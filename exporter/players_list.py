import requests
from lib import generate_file_name,write_to_json
from load_env import get_env_vars
from lib import logger

config = get_env_vars()

def get_api_response(api_config:tuple):
    """
    This method is used to get the player list from the Api
    :param api_config:  config
    :return: list of players in json format
    """
    OFFSET = 0
    BATCH = 25
    iteration = 0
    while True:
        try:
            url = (
                f"{api_config.SERIES_LIST_API_URL}apikey={api_config.API_KEY}&offset={OFFSET}"
            )
            response = requests.get(url)
            if response.status_code == 200:
                response_json = response.json().get("data", [])
                if not response_json:
                    logger.info("No more data returned.")
                    break
                OFFSET += len(response_json)
                iteration += 1
                file_name = generate_file_name("series_list", "json", iteration)
                write_to_json(response_json, file_name)
                logger.info(f"Saved batch {iteration} to {file_name}")
                if len(response_json) < BATCH:
                    logger.info("Final batch received. Ending loop.")
                    break
            else:
                logger.exception(f"Request failed with status code {response.status_code}: {url}")
                break
        except Exception as e:
            logger.error(f"An exception occured; {e}")
            raise e
    return "Successfully completed the player list extraction from Api"

get_api_response(config)