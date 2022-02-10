import os
import pandas as pd
import sqlite3
from itertools import repeat
from multiprocessing import Pool
from typing import List

ROW_ORDER_COLUMN = '_sqlite_id' # for preserving row order, otherwise unused


def get_metadata_id_column(metadata_file:str, id_columns:List[str]):
    """Returns the first column in `id_columns` that is present in the metadata.

    Raises a `ValueError` when none of `id_columns` are found.
    """
    read_csv_kwargs = {
        "sep": '\t',
        "engine": "c",
        "skipinitialspace": True,
        "dtype": 'string',
    }
    row = pd.read_csv(
        metadata_file,
        nrows=1,
        **read_csv_kwargs,
    )
    for col in id_columns:
        if col in row.columns:
            return col
    raise ValueError(f"None of the possible id columns ({id_columns!r}) were found in the metadata's columns {tuple(row.columns)!r}")


def load_tsv(tsv_file:str, db_file:str, connection:sqlite3.Connection, table_name:str,
        header=True, names=[], dtype='string', n_jobs=1, chunk_size=100000):
    """Reads tabular data from a file."""
    read_csv_kwargs = {
        "sep": '\t',
        "engine": "c",
        "skipinitialspace": True,
        "dtype": dtype,
        "na_filter": False,
        "chunksize": chunk_size,
    }
    if not header and not names:
        raise ValueError()
    if not header and names:
        read_csv_kwargs['header'] = None
        read_csv_kwargs['names'] = names
    df_chunks = pd.read_csv(tsv_file, **read_csv_kwargs)
    if db_file == ':memory:':
        # in-memory databases can't use multiprocessing
        # and a new connection would create a new database https://stackoverflow.com/a/15720872
        # instead, use existing connection for same memory db
        for chunk in df_chunks:
            chunk.to_sql(table_name, connection, if_exists='append', index=True, index_label=ROW_ORDER_COLUMN)
    else:
        with Pool(processes=n_jobs) as pool:
            pool.starmap(_chunk_to_sql, zip(df_chunks, repeat(db_file), repeat(table_name)))


def _chunk_to_sql(chunk:pd.DataFrame, db_file:str, table_name:str):
    """Adds DataFrame contents to a table in a database file."""
    # connection not passed as param because it is not thread safe (at least by default)
    connection = sqlite3.connect(db_file, timeout=15) # to reduce OperationalError: database is locked
    chunk.to_sql(table_name, connection, if_exists='append', index=True, index_label=ROW_ORDER_COLUMN)


def cleanup(database:str):
    """Removes the database file if present."""
    try:
        os.remove(database)
    except FileNotFoundError:
        pass