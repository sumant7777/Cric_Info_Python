from snowflake_connector import create_sesion
from snowflake.snowpark.functions import col
from lib import logger


def get_id(db_name: str, schema_name: str, table_name: str, column_name: str) -> list:
    """
    this method is used to get the list of id's in the table
    :param db_name: Database name
    :param schema_name: Schema name
    :param table_name: Table name
    :param column_name: column name
    :return: list of id's
    """
    session = create_sesion()
    table_df = (
        session.table(f"{db_name}.{schema_name}.{table_name}")
        .select(col(column_name))
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
