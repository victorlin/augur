import pandas as pd
from sqlite3 import Connection

DEFAULT_DB_FILE = 'test.sqlite3'


def load_tsv(connection:Connection, tsv_file:str, table_name:str, db_file:str=DEFAULT_DB_FILE, header=True, names=[]):
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    if header:
        df_chunks = pd.read_csv(tsv_file, sep='\t', chunksize=1000)
        for chunk in df_chunks:
            chunk.to_sql(table_name, connection, if_exists='append', index=False)
