import duckdb

DEFAULT_DB_FILE = 'test.duckdb'

METADATA_TABLE_NAME = 'metadata'
SEQUENCE_INDEX_TABLE_NAME = 'sequence_index'
PRIORITIES_TABLE_NAME = 'priorities'
DATE_TABLE_NAME = 'metadata_date_expanded'

FILTERED_VIEW_NAME = 'metadata_filtered'
EXTENDED_VIEW_NAME = 'metadata_filtered_extended'


def load_tsv(tsv_file:str, table_name:str, db_file:str=DEFAULT_DB_FILE, header=True, names=[]):
    connection = duckdb.connect(db_file)
    table_from_tsv = f"read_csv_auto('{tsv_file}', delim='\t', header={header})"
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_from_tsv}")
    if not header and names:
        for i, name in enumerate(names):
            connection.execute(f"ALTER TABLE {table_name} RENAME COLUMN column{i} TO {name}")
    connection.close()


def query_df(query:str, db_file:str=DEFAULT_DB_FILE):
    connection = duckdb.connect(db_file)
    return connection.execute(query).fetch_df()

