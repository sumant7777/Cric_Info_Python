import json
from datetime import datetime

def generate_timestamp_format():
    """
    this method is used to generate the timestamp format
    :return: timestamp_format
    """
    return datetime.now().strftime("%Y%m%d%H%M%S")


def generate_file_name(file_name, file_type, iteration):
    """
    this method is used to generate the file name
    :param file_name: str
    :param file_type: str
    :param iteration: int
    :return: str
    """
    return f"data/{file_name}/{generate_timestamp_format()}_{file_name}_{str(iteration).zfill(5)}.{file_type}"


def write_to_json(data, file_name):
    """
    This method is used to write data to json file
    :param data: json_data
    :param file_name: str
    """
    try:
        print(f"Writing api data into json file{file_name}")
        with open(f"{file_name}", "w") as json_file:
            json.dump(data, json_file, indent=4)
    except Exception as e:
        print(f"An exception occurred{e}")
        raise e
