import requests
from lib import generate_file_name,write_to_json
from load_env import get_env_vars
from s3 import upload_file_s3
from get_id import get_id


player_id_list = get_id("cric_info","cric_data","player_info","PLAYER_ID")
player_stats_id_list = get_id("cric_info","cric_data","player_stats","PLAYER_ID")

player_id_list = list(set(player_id_list)-set(player_stats_id_list))
player_id_list = player_id_list[:100]

config = get_env_vars()

def get_player_info(api_config,id_list):
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
                        print(f"No more data returned for player ID {player_id}.")
                        break

                    OFFSET += len(response_json)
                    iteration += 1
                    file_name = generate_file_name("player_stats", "json", iteration)
                    write_to_json(response_json, file_name)
                    upload_file_s3(file_name,api_config.CRIC_INFO_BUCKET,api_config.PLAYER_STATS_S3_DEST)
                    print(f"Saved batch {iteration} to {file_name}")
                    if len(response_json) < BATCH:
                        print("Final batch received")
                        break
                else:
                    print(f"Request failed with status code {response.status_code}: {url}")
                    break
            except Exception as e:
                print(f"An exception occurred for player ID {player_id}: {e}")
                break

get_player_info(config,player_id_list)
