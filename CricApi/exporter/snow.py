from pathlib import Path
from util import logger
from load_env import get_env_vars
from snowflake.snowpark import Session
import sqlparse
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSQLException

config = get_env_vars()


def create_session():
    try:
        connection_parameters = {
            "account": config.SF_ACCOUNT,
            "user": config.SF_USER_NAME,
            "password": config.SF_PASSWORD,
            "role": config.SF_ROLE,
            "warehouse": config.SF_WH,
            "database": config.SF_DATABASE,
            "schema": config.SF_SCHEMA,
        }

        # Create a session
        session = Session.builder.configs(connection_parameters).create()
        logger.info("created a snowflake session")
        return session
    except SnowparkSQLException as e:
        logger.error(f"An error occurred while creating the Snowflake session: {e}")
        raise e


def close_session(session: Session):
    """
    Closes the Snowflake session.
    :param session: Snowflake session object
    """
    try:
        session.close()
        logger.info("Closed the Snowflake session")
    except SnowparkSQLException as e:
        logger.error(f"An error occurred while closing the Snowflake session: {e}")
        raise e


def get_id(db_name: str, schema_name: str, table_name: str, column_name: str) -> list:
    """
    this method is used to get the list of id's in the table
    :param db_name: db_name
    :param schema_name: schema_name
    :param table_name: table_name
    :param column_name: column_name
    :return: list
    """
    session = create_session()
    table_df = (
        session.table(f"{db_name}.{schema_name}.{table_name}")
        .select(col(column_name))
        .distinct()
        .order_by(column_name)
        .to_pandas()
    )
    id_list = table_df[column_name].tolist()
    logger.info(
        "Getting the list of id's for the table {}.{}.{}".format(
            db_name, schema_name, table_name
        )
    )
    return id_list


def get_count(db_name: str, schema_name: str, table_name: str, column_name: str) -> int:
    """
    this method is used to get the count of rows in the table
    :param db_name: db_name
    :param schema_name: schema_name
    :param table_name: table_name
    :param column_name: column_name
    :return: int
    """
    session = create_session()
    count = (
        session.table(f"{db_name}.{schema_name}.{table_name}")
        .select(col(column_name))
        .distinct()
        .count()
    )
    logger.info(
        "Getting the count of rows for the table {}.{}.{}".format(
            db_name, schema_name, table_name
        )
    )
    return count


def is_path(string):
    p = Path(string)
    return p.is_absolute() or p.parent != Path(".")


def read_sql_file(file_path: str) -> str:
    """
    Reads a SQL file and returns its content.
    :param file_path: Path to the SQL file
    :return: Content of the SQL file as a string
    """
    try:
        with open(file_path, "r") as file:
            sql_content = file.read()
        logger.info(f"Read SQL file: {file_path}")
        return sql_content
    except Exception as e:
        logger.error(f"An error occurred while reading the SQL file: {e}")
        raise e


def execute_query(session: Session, query: str):
    """
    Executes a SQL query on the Snowflake session.
    :param session: Snowflake session object
    :param query: SQL query or sql file to execute
    :return: Result of the query execution
    """
    try:
        if is_path(query):
            logger.info(f"reading SQL file: {query}")
            query = read_sql_file(query).strip()
        else:
            query = query.strip()
        query_list = sqlparse.split(query)
        query_result = []
        if query_list:
            for query in query_list:
                result = session.sql(query).collect()
                logger.info(
                    f"Executed query: {sqlparse.format(query, reindent=True, keyword_case='upper').strip()}"
                )
                query_result.extend(result)
        return query_result
    except SnowparkSQLException as e:
        close_session(session)
        logger.error(f"An error occurred while executing the query:{query}: {e}")
        raise e
