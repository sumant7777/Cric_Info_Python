from dotenv import load_dotenv,dotenv_values
from collections import namedtuple
import os

load_dotenv()

def get_env_vars():
    env_vars = dict(dotenv_values('.env'))

    Cric_Api_Config = namedtuple("Cric_Api_Config", env_vars.keys())
    return Cric_Api_Config(**env_vars)
    