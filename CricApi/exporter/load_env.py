from dotenv import load_dotenv, dotenv_values
from collections import namedtuple
import os

load_dotenv()


def get_env_vars():
    """
    Load environment variables from a .env file and return them as a named tuple.
    :return: namedtuple with environment variables
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
    env_path = os.path.join(project_root, ".env")
    env_vars = dict(dotenv_values(env_path))
    Cric_Api_Config = namedtuple("Cric_Api_Config", env_vars.keys())
    return Cric_Api_Config(**env_vars)
