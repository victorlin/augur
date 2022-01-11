import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection

DEFAULT_DB_FILE = 'test.duckdb'

METADATA_TABLE_NAME = 'metadata'
SEQUENCE_INDEX_TABLE_NAME = 'sequence_index'
PRIORITIES_TABLE_NAME = 'priorities'
DATE_TABLE_NAME = 'metadata_date_expanded'
GROUP_SIZES_TABLE_NAME = 'group_sizes'

FILTERED_VIEW_NAME = 'metadata_filtered'
EXTENDED_VIEW_NAME = 'metadata_filtered_extended'


def load_tsv(connection:DuckDBPyConnection, tsv_file:str, table_name:str, db_file:str=DEFAULT_DB_FILE, header=True, names=[]):
    # column_types = get_column_types(connection, tsv_file)
    # table_from_tsv = f"read_csv('{tsv_file}', delim='\t', header={header}, columns={repr(column_types)})"
    table_from_tsv = f"read_csv_auto('{tsv_file}', delim='\t', header={header})"
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_from_tsv}")
    if not header and names:
        for i, name in enumerate(names):
            connection.execute(f"ALTER TABLE {table_name} RENAME COLUMN column{i} TO {name}")


def get_column_types(connection:DuckDBPyConnection, tsv_file:str):
    tmp_view = 'tmp'
    df = pd.read_csv(tsv_file, sep='\t', nrows=10)
    connection.register(tmp_view, df)
    df_table_info = connection.execute(f"PRAGMA table_info('{tmp_view}');").df()
    connection.unregister(tmp_view)
    return dict(zip(df_table_info['name'], df_table_info['type']))
