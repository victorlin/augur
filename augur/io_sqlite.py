import pandas as pd
import sqlite3
from itertools import repeat
from multiprocessing import Pool
from sqlite3 import Connection

DEFAULT_DB_FILE = 'test.sqlite3'


def load_tsv(connection:Connection, tsv_file:str, table_name:str, header=True, names=[], n_jobs=1):
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
    with Pool(processes=n_jobs) as pool:
        pool.starmap(chunk_to_sql, zip(df_chunks, repeat(table_name)))


def chunk_to_sql(chunk:pd.DataFrame, table_name:str):
    connection = sqlite3.connect(DEFAULT_DB_FILE, timeout=15) # to reduce OperationalError: database is locked
    chunk.to_sql(table_name, connection, if_exists='append', index=False)
