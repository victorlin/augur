import os
import pandas as pd
import sqlite3

from . import TabularFileLoaderBase

ROW_ORDER_COLUMN = '_sqlite_id' # for preserving row order, otherwise unused


class TabularFileLoaderSQLite(TabularFileLoaderBase):
    def __init__(self, file:str, connection:sqlite3.Connection, table_name:str, header=True, names=[]):
        super().__init__(file, header, names)
        self.connection = connection
        self.table_name = table_name

    def load(self):
        # insert ROW_ORDER_COLUMN with dummy value of 1
        self.df_head.insert(0, ROW_ORDER_COLUMN, value=1)
        self.columns = self.df_head.columns
        # create table with schema defined by self.df_head, including additional ROW_ORDER_COLUMN
        with self.connection:
            create_table_statement = pd.io.sql.get_schema(self.df_head, self.table_name, con=self.connection)
            self.connection.execute(create_table_statement)

        # insert rows
        insert_statement = f"""
            INSERT INTO {self.table_name}
            VALUES ({','.join(['?' for _ in self.columns])})
        """
        rows = self._iter_indexed_rows() # this relies on ROW_ORDER_COLUMN as first column
        try:
            with self.connection:
                self.connection.executemany(insert_statement, rows)
        except sqlite3.ProgrammingError as e:
            raise ValueError(f'Failed to load {self.file}.') from e


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
