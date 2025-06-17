import json
from datetime import datetime
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


def generate_timestamp_format():
    """
    this method is used to generate the timestamp format
    :return: timestamp_format
    """
    return datetime.now().strftime("%Y%m%d%H%M%S")


def generate_file_name(file_name: str, file_type: str, iteration: int) -> str:
    """
    this method is used to generate the file name
    :param file_name: name of the file
    :param file_type: type of the file (e.g., json, csv)
    :param iteration: iteration number for the file
    :return: complete file name with timestamp and iteration
    """
    return f"data/{file_name}/{generate_timestamp_format()}_{file_name}_{str(iteration).zfill(5)}.{file_type}"


def write_to_json(data: json, file_name: str) -> None:
    """
    This method is used to write data to json file
    :param data: json_data
    :param file_name: name of the file to write
    """
    try:
        logger.info(f"Writing api data into json file{file_name}")
        with open(f"{file_name}", "w") as json_file:
            json.dump(data, json_file, indent=4)
    except Exception as e:
        logger.error(f"An exception occurred{e}")
        raise e
