import os
import pandas as pd
import sqlite3

ROW_ORDER_COLUMN = '_sqlite_id' # for preserving row order, otherwise unused


def load_tsv(tsv_file:str, connection:sqlite3.Connection, table_name:str, header=True, names=[], dtype='string', n_jobs=1):
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
    for chunk in df_chunks:
        chunk.to_sql(table_name, connection, if_exists='append', index=True, index_label=ROW_ORDER_COLUMN)


def cleanup(database:str):
    try:
        os.remove(database)
    except FileNotFoundError:
        pass