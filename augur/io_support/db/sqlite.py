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


def load_tsv(tsv_file:str, connection:sqlite3.Connection, table_name:str, header=True, names=[]):
    """Loads tabular data from a file."""
    read_csv_kwargs = {
        "sep": '\t',
        "engine": "c",
        "skipinitialspace": True,
    }
    if not header and not names:
        raise ValueError()
    if not header and names:
        read_csv_kwargs['header'] = None
        read_csv_kwargs['names'] = names
    # infer data types from first 100 rows using pandas
    df = pd.read_csv(
        tsv_file,
        nrows=100,
        **read_csv_kwargs,
    )
    df.insert(0, ROW_ORDER_COLUMN, 1)

    cur = connection.cursor()
    create_table_statement = pd.io.sql.get_schema(df, table_name, con=connection)
    cur.execute(create_table_statement)

    # don't use pandas to_sql since it keeps data in memory
    # and using parallelized chunks does not work well with SQLite limited concurrency
    try:
        with myopen(tsv_file) as f:
            reader = csv.reader(f, delimiter='\t')  # TODO: detect delimiter
            if header:
                next(reader)
            for i, row in enumerate(reader):
                indexed_row = [i] + row
                cur.executemany(f"""
                    INSERT INTO {table_name}
                    VALUES ({','.join(['?' for _ in df.columns])})
                """, [indexed_row])
    except sqlite3.ProgrammingError as e:
        raise ValueError(f'Failed to load {tsv_file}.') from e



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
