import os
import pandas as pd
import sqlite3
from itertools import repeat
from multiprocessing import Pool

DEFAULT_DB_FILE = 'test.sqlite3'
ROW_ORDER_COLUMN = '_sqlite_id'


def load_tsv(tsv_file:str, table_name:str, header=True, names=[], dtype='string', n_jobs=1):
    read_csv_kwargs = {
        "sep": '\t',
        "engine": "c",
        "skipinitialspace": True,
        "dtype": dtype,
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
    chunk.to_sql(table_name, connection, if_exists='append', index=True, index_label=ROW_ORDER_COLUMN)


def cleanup():
    try:
        os.remove(DEFAULT_DB_FILE)
    except FileNotFoundError:
        pass