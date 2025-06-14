import json


def generate_file_name(file_name, file_type, iteration):
    return f"data/{file_name}/{file_name}_{str(iteration).zfill(5)}.{file_type}"


def write_to_json(data, file_name):
    try:
        print(f"Writing api data into json file{file_name}")
        with open(f"{file_name}", "w") as json_file:
            json.dump(data, json_file, indent=4)
    except Exception as e:
        print(f"An exception occurred{e}")
        raise e