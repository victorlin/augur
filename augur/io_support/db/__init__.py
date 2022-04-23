import csv
import pandas as pd
from typing import List

from augur.utils import myopen


def get_metadata_id_column(metadata_file:str, id_columns:List[str]):
    """Returns the first column in `id_columns` that is present in the metadata.

    Raises a `ValueError` when none of `id_columns` are found.
    """
    metadata_columns = _get_column_names(metadata_file)
    for col in id_columns:
        if col in metadata_columns:
            return col
    raise ValueError(f"None of the possible id columns ({id_columns!r}) were found in the metadata's columns {tuple(metadata_columns)!r}")


def _get_column_names(file:str):
    """Get column names using pandas."""
    delimiter = get_delimiter(file)
    read_csv_kwargs = {
        "sep": delimiter,
        "engine": "c",
        "skipinitialspace": True,
        "dtype": 'string',
    }
    row = pd.read_csv(
        file,
        nrows=1,
        **read_csv_kwargs,
    )
    return list(row.columns)


def get_delimiter(file:str, delimiters:List[str]=[',', '\t']):
    """Infer tabular delimiter from first line of a file."""
    with myopen(file) as f:
        try:
            dialect = csv.Sniffer().sniff(f.readline(), delimiters=delimiters)
        except csv.Error:
            # this can happen for single-column files, e.g. VCF sequence indexes
            return '\t'
    return dialect.delimiter


class TabularFileLoaderBase:
    def __init__(self, file:str, header=True, names=[]):
        self.file = file
        self.header = header
        self.names = names

        self.delimiter = get_delimiter(self.file)
        self.df_head = self._get_pd_df_head_100()

    def _get_pd_df_head_100(self) -> pd.DataFrame:
        """Get a pandas DataFrame representation of the first 100 rows in the file.
        
        This is used later to:
        1. Define column names
        2. Infer data types for table creation

        TODO: describe header/names
        """
        read_csv_kwargs = {
            "sep": self.delimiter,
            "engine": "c",
            "skipinitialspace": True,
        }
        if not self.header and not self.names:
            raise ValueError()
        if not self.header and self.names:
            read_csv_kwargs['header'] = None
            read_csv_kwargs['names'] = self.names
        return pd.read_csv(
            self.file,
            nrows=100,
            **read_csv_kwargs,
        )

    def _iter_indexed_rows(self):
        """Yield rows from a tabular file with an additional first column for row number."""
        with myopen(self.file) as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            if self.header:
                next(reader)
            for i, row in enumerate(reader):
                if not row:
                    continue
                yield [i] + row
