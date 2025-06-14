import requests
from lib import config,generate_file_name,write_to_json


def get_api_response(config):
    OFFSET = 0
    BATCH = 25
    iteration = 0
    while True:
        try:
            url = (
                f"{config.series_list_url}?apikey={config.api_key}&offset={OFFSET}"
            )
            response = requests.get(url)
            if response.status_code == 200:
                response_json = response.json().get("data", [])
                if not response_json:
                    print("No more data returned.")
                    break
                OFFSET += len(response_json)
                iteration += 1
                file_name = generate_file_name("series_list", "json", iteration)
                write_to_json(response_json, file_name)
                print(f"Saved batch {iteration} to {file_name}")
                if len(response_json) < BATCH:
                    print("Final batch received. Ending loop.")
                    break
            else:
                print(f"Request failed with status code {response.status_code}: {url}")
                break  # Exit the loop if a bad status is returned
        except Exception as e:
            print(f"An exception occured; {e}")
