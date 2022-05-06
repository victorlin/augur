import os
import pandas as pd
import sqlite3

from . import TabularFileLoaderBase

ROW_ORDER_COLUMN = '_sqlite_id' # for preserving row order, otherwise unused


class TabularFileLoaderSQLite(TabularFileLoaderBase):
    """Class for loading tabular files into a SQLite database.

    Extends the base class with additional SQLite-specific attributes - see __init__.
    """
    def __init__(self, file:str, connection:sqlite3.Connection, table_name:str, header=True, names=[]):
        super().__init__(file, header, names)
        self.connection = connection
        "sqlite3 Connection object."
        self.table_name = table_name
        "Table name used to load tabular file contents into the database."

        self.columns = None
        "Column names to be used in the table."

    def load(self):
        # Insert ROW_ORDER_COLUMN as first column with dummy value of 1 so the schema matches what's given by self._iter_indexed_rows().
        self.df_head.insert(0, ROW_ORDER_COLUMN, value=1)
        self.columns = self.df_head.columns
        # Create table with schema defined by self.df_head, including additional ROW_ORDER_COLUMN.
        with self.connection:
            create_table_statement = pd.io.sql.get_schema(self.df_head, self.table_name, con=self.connection)
            self.connection.execute(create_table_statement)

        insert_statement = f"""
            INSERT INTO {self.table_name}
            VALUES ({','.join(['?' for _ in self.columns])})
        """
        rows = self._iter_indexed_rows()
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
