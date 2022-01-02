import duckdb

DEFAULT_DB_FILE = 'test.duckdb'
TABLE_NAME = 'metadata'

def load_metadata(metadata_file:str, db_file:str=DEFAULT_DB_FILE):
    connection = duckdb.connect(db_file)
    table_from_tsv = f"read_csv_auto('{metadata_file}', delim='\t', header=True)"
    connection.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    connection.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM {table_from_tsv}")
    connection.close()


def query_df(query:str, db_file:str=DEFAULT_DB_FILE):
    connection = duckdb.connect(db_file)
    return connection.execute(query).fetch_df()

