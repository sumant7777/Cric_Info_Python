from snowflake_connector import create_sesion


def get_id(db_name, schema_name, table_name, column_name):

    session = create_sesion()
    table_df = session.table(f"{db_name}.{schema_name}.{table_name}").to_pandas()
    id_list = table_df[column_name].tolist()
    print("Getting the list of id's for the table")
    return id_list
