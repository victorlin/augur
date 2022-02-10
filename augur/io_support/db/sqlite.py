import csv
import os
import pandas as pd
import sqlite3
from typing import Dict, List

from augur.utils import myopen

ROW_ORDER_COLUMN = '_sqlite_id' # for preserving row order, otherwise unused


def get_metadata_id_column(metadata_file:str, id_columns:List[str]):
    """Returns the first column in `id_columns` that is present in the metadata.

    Raises a `ValueError` when none of `id_columns` are found.
    """
    metadata_columns = _get_column_names(metadata_file)
    for col in id_columns:
        if col in metadata_columns:
            return col
    raise ValueError(f"None of the possible id columns ({id_columns!r}) were found in the metadata's columns {tuple(metadata_columns)!r}")


def load_tsv(tsv_file:str, connection:sqlite3.Connection, table_name:str,
        header=True, names=[], dtypes:Dict[str,str]=None):
    """Loads tabular data from a file."""
    cur = connection.cursor()
    if not header:
        if not names:
            raise ValueError()
        column_names = names
    else:
        column_names = _get_column_names(tsv_file)
    column_names = [ROW_ORDER_COLUMN] + column_names
    if dtypes:
        dtypes[ROW_ORDER_COLUMN] = 'INTEGER'
        cur.execute(f"""
            CREATE TABLE {table_name} ({','.join([f'"{col}" {dtypes[col]}' for col in column_names])})
        """)
        # TODO: STRICT typing https://www.sqlite.org/stricttables.html
    else:
        cur.execute(f"""
            CREATE TABLE {table_name} ({','.join([f'"{col}"' for col in column_names])})
        """)

    with myopen(tsv_file) as f:
        reader = csv.reader(f, delimiter='\t')  # TODO: detect delimiter
        if header:
            next(reader)
        for i, row in enumerate(reader):
            indexed_row = [i] + row
            cur.executemany(f"""
                INSERT INTO {table_name}
                VALUES ({','.join(['?' for _ in column_names])})
            """, [indexed_row])


def cleanup(database:str):
    """Removes the database file if present."""
    try:
        os.remove(database)
    except FileNotFoundError:
        pass


def _get_column_names(tsv_file:str):
    """Get column names using pandas."""
    read_csv_kwargs = {
        "sep": '\t',
        "engine": "c",
        "skipinitialspace": True,
        "dtype": 'string',
    }
    row = pd.read_csv(
        tsv_file,
        nrows=1,
        **read_csv_kwargs,
    )
    return list(row.columns)
