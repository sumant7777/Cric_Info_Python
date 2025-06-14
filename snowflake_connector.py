from snowflake.snowpark import Session
from load_env import get_env_vars

config = get_env_vars()

def create_sesion():
    connection_parameters = {
        "account": config.SF_ACCOUNT,
        "user": config.SF_USER_NAME,
        "password": config.SF_PASSWORD,
        "role": config.SF_ROLE,
        "warehouse": config.SF_WH,
        "database": config.SF_DATABASE,
        "schema": config.SF_SCHEMA
    }

    # Create a session
    session = Session.builder.configs(connection_parameters).create()
    print("created a snowflake session")
    return session
