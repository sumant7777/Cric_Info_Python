from snowflake_connector import create_sesion
from snowflake.snowpark.functions import col

def get_id(db_name, schema_name, table_name, column_name):
    """
    this method is used to get the list of id's in the table
    :param db_name: str
    :param schema_name: str
    :param table_name: str
    :param column_name: str
    :return: list
    """
    session = create_sesion()
    table_df = session.table(f"{db_name}.{schema_name}.{table_name}").select(col(column_name)).order_by(column_name).to_pandas()
    id_list = table_df[column_name].tolist()
    print("Getting the list of id's for the table {}.{}.{}".format(db_name, schema_name, table_name))
    return id_list
