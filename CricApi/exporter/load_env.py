from dotenv import load_dotenv, dotenv_values
from collections import namedtuple

load_dotenv()


def get_env_vars():
    """
    Load environment variables from a .env file and return them as a named tuple.
    :return: namedtuple with environment variables
    """
    env_vars = dict(dotenv_values(".env"))

    Cric_Api_Config = namedtuple("Cric_Api_Config", env_vars.keys())
    return Cric_Api_Config(**env_vars)
