import pandas as pd
from sqlite3 import Connection

DEFAULT_DB_FILE = 'test.sqlite3'


def load_tsv(connection:Connection, tsv_file:str, table_name:str, db_file:str=DEFAULT_DB_FILE, header=True, names=[]):
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    read_csv_kwargs = {
        "sep": '\t',
        "engine": "c",
        "skipinitialspace": True,
        "dtype": "string",
        "na_filter": False,
        "chunksize": 100000,
    }
    if not header and not names:
        raise ValueError()
    if not header and names:
        read_csv_kwargs['header'] = None
        read_csv_kwargs['names'] = names
    df_chunks = pd.read_csv(tsv_file, **read_csv_kwargs)
    for chunk in df_chunks:
        chunk.to_sql(table_name, connection, if_exists='append', index=False)
