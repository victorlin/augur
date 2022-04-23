import os
import pandas as pd
import sqlite3

from . import get_delimiter, iter_indexed_rows

ROW_ORDER_COLUMN = '_sqlite_id' # for preserving row order, otherwise unused


def load_tsv(file:str, connection:sqlite3.Connection, table_name:str, header=True, names=[]):
    """Loads tabular data from a file."""
    delimiter = get_delimiter(file)
    read_csv_kwargs = {
        "sep": delimiter,
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
        file,
        nrows=100,
        **read_csv_kwargs,
    )
    df.insert(0, ROW_ORDER_COLUMN, 1)

    with connection:
        create_table_statement = pd.io.sql.get_schema(df, table_name, con=connection)
        connection.execute(create_table_statement)

    # don't use pandas to_sql since it keeps data in memory
    # and using parallelized chunks does not work well with SQLite limited concurrency
    insert_statement = f"""
        INSERT INTO {table_name}
        VALUES ({','.join(['?' for _ in df.columns])})
    """
    rows = iter_indexed_rows(file, header)
    try:
        with connection:
            connection.executemany(insert_statement, rows)
    except sqlite3.ProgrammingError as e:
        raise ValueError(f'Failed to load {file}.') from e


def cleanup(database:str):
    """Removes the database file if present."""
    try:
        os.remove(database)
    except FileNotFoundError:
        pass


def sanitize_identifier(identifier:str):
    """Sanitize a SQLite identifier.
    
    1. Escape existing double quotes
    2. Wrap inside double quotes
    """
    identifier = identifier.replace('"', '""')
    return f'"{identifier}"'
