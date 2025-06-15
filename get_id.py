from snowflake_connector import create_sesion
from snowflake.snowpark.functions import col

def get_id(db_name, schema_name, table_name, column_name):

    session = create_sesion()
    table_df = session.table(f"{db_name}.{schema_name}.{table_name}").select(col(column_name)).order_by(column_name).to_pandas()
    id_list = table_df[column_name].tolist()
    print("Getting the list of id's for the table {}.{}.{}".format(db_name, schema_name, table_name))
    return id_list
