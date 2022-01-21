import re
from typing import List
import pandas as pd
import sqlite3
import argparse

from augur.filter_db import FilterDB
from .io_sqlite import load_tsv, DEFAULT_DB_FILE
from .utils import read_strains
from .filter_subsample_helpers import get_sizes_per_group


METADATA_TABLE_NAME = 'metadata'
SEQUENCE_INDEX_TABLE_NAME = 'sequence_index'
PRIORITIES_TABLE_NAME = 'priorities'
DATE_TABLE_NAME = 'metadata_date_expanded'
METADATA_FILTER_REASON_TABLE_NAME = 'metadata_filtered_reason'
GROUP_SIZES_TABLE_NAME = 'group_sizes'
FILTERED_TABLE_NAME = 'metadata_filtered'
SUBSAMPLED_TABLE_NAME = 'metadata_subsampled'
OUTPUT_METADATA_TABLE_NAME = 'metadata_output'

EXTENDED_VIEW_NAME = 'metadata_filtered_extended'
SUBSAMPLE_STRAINS_VIEW_NAME = 'subsample_strains'

DEFAULT_DATE_COL = 'date'
FILTER_REASON_COL = 'filter_reason'
EXCLUDE_COL = 'exclude'
INCLUDE_COL = 'force_include'
DUMMY_COL = 'dummy'
GROUP_SIZE_COL = 'size'
STRAIN_COL = 'strain'
PRIORITY_COL = 'priority'


class FilterSQLite(FilterDB):
    def __init__(self, args:argparse.Namespace):
        super().__init__(args)

    def db_connect(self):
        self.connection = sqlite3.connect(DEFAULT_DB_FILE)
        self.cur = self.connection.cursor()

    def db_load_metadata(self):
        load_tsv(self.connection, self.args.metadata, METADATA_TABLE_NAME)
        self.cur.execute(f"""
            CREATE UNIQUE INDEX idx_{METADATA_TABLE_NAME}_{STRAIN_COL}
            ON {METADATA_TABLE_NAME} ({STRAIN_COL})
        """)

    def db_load_sequence_index(self, path):
        load_tsv(self.connection, path, SEQUENCE_INDEX_TABLE_NAME)

    def db_has_date_col(self):
        columns = {i[1] for i in self.cur.execute(f'PRAGMA table_info({METADATA_TABLE_NAME})')}
        return (DEFAULT_DATE_COL in columns)

    def db_create_date_table(self):
        self.connection.create_function('get_year', 1, get_year)
        self.connection.create_function('get_month', 1, get_month)
        self.connection.create_function('get_day', 1, get_day)
        self.connection.create_function('get_date_min', 1, get_date_min)
        self.connection.create_function('get_date_max', 1, get_date_max)
        self.connection.execute(f"DROP TABLE IF EXISTS {DATE_TABLE_NAME}")
        self.cur.execute(f"""CREATE TABLE {DATE_TABLE_NAME} AS
            SELECT
                {STRAIN_COL},
                {DEFAULT_DATE_COL},
                get_year(date) as year,
                get_month(date) as month,
                get_day(date) as day,
                date(get_date_min(date)) as date_min,
                date(get_date_max(date)) as date_max
            FROM {METADATA_TABLE_NAME}
        """)

    def filter_by_exclude_all(self):
        """Exclude all strains regardless of the given metadata content.

        This is a placeholder function that can be called as part of a generalized
        loop through all possible functions.

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        return 'True'

    def filter_exclude_strains(self, exclude_file):
        """Exclude the given set of strains from the given metadata.

        Parameters
        ----------
        exclude_file : str
            Filename with strain names to exclude from the given metadata

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        excluded_strains = read_strains(exclude_file)
        excluded_strains = [f"'{strain}'" for strain in excluded_strains]
        return f"{STRAIN_COL} IN ({','.join(excluded_strains)})"

    def parse_filter_query(self, query):
        """Parse an augur filter-style query and return the corresponding column,
        operator, and value for the query.

        Parameters
        ----------
        query : str
            augur filter-style query following the pattern of `"property=value"` or `"property!=value"`

        Returns
        -------
        str :
            Name of column to query
        callable :
            Operator function to test equality or non-equality of values
        str :
            Value of column to query

        >>> parse_filter_query("property=value")
        ('property', '=', 'value')
        >>> parse_filter_query("property!=value")
        ('property', '!=', 'value')

        """
        column, value = re.split(r'!?=', query)
        op = '='
        if "!=" in query:
            op = '!='

        return column, op, value

    def filter_exclude_where(self, exclude_where):
        """Exclude all strains from the given metadata that match the given exclusion query.

        Unlike pandas query syntax, exclusion queries should follow the pattern of
        `"property=value"` or `"property!=value"`. Additionally, this filter treats
        all values like lowercase strings, so we convert all values to strings first
        and then lowercase them before testing the given query.

        Parameters
        ----------
        exclude_where : str
            Filter query used to exclude strains

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        column, op, value = self.parse_filter_query(exclude_where)
        return f"""{STRAIN_COL} IN (
            SELECT {STRAIN_COL}
            FROM {METADATA_TABLE_NAME}
            WHERE {column} {op} '{value}'
        )
        """

    def filter_by_query(self, query):
        return f"""{STRAIN_COL} IN (
            SELECT {STRAIN_COL}
            FROM {METADATA_TABLE_NAME}
            WHERE NOT {query}
        )
        """

    def exclude_by_ambiguous_date(self, ambiguity="any"):
        """Filter metadata in the given pandas DataFrame where values in the given date
        column have a given level of ambiguity.

        Determine ambiguity hierarchically such that, for example, an ambiguous
        month implicates an ambiguous day even when day information is available.

        Parameters
        ----------
        ambiguity : str
            Level of date ambiguity to filter metadata by

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        if ambiguity == 'year':
            return f"""{STRAIN_COL} IN (
                SELECT {STRAIN_COL}
                FROM {DATE_TABLE_NAME}
                WHERE year IS NULL
            )"""
        if ambiguity == 'month':
            return f"""{STRAIN_COL} IN (
                SELECT {STRAIN_COL}
                FROM {DATE_TABLE_NAME}
                WHERE month IS NULL OR year IS NULL
            )"""
        if ambiguity == 'day' or ambiguity == 'any':
            return f"""{STRAIN_COL} IN (
                SELECT {STRAIN_COL}
                FROM {DATE_TABLE_NAME}
                WHERE day IS NULL OR month IS NULL OR year IS NULL
            )"""

    def exclude_by_min_date(self, min_date):
        """Filter metadata by minimum date.

        Parameters
        ----------
        min_date : float
            Minimum date

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        return f"""{STRAIN_COL} IN (
            SELECT {STRAIN_COL}
            FROM {DATE_TABLE_NAME}
            WHERE date_min < '{min_date}' OR date_min IS NULL
        )"""

    def exclude_by_max_date(self, max_date):
        """Filter metadata by maximum date.

        Parameters
        ----------
        max_date : float
            Maximum date

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        return f"""{STRAIN_COL} IN (
            SELECT {STRAIN_COL}
            FROM {DATE_TABLE_NAME}
            WHERE date_max > '{max_date}' OR date_max IS NULL
        )"""

    def exclude_by_sequence_index(self):
        """Filter metadata by presence of corresponding entries in a given sequence
        index. This filter effectively intersects the strain ids in the metadata and
        sequence index.

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        # TODO: consider JOIN vs subquery if performance issues https://stackoverflow.com/q/3856164
        return f"{STRAIN_COL} NOT IN (SELECT {STRAIN_COL} FROM {SEQUENCE_INDEX_TABLE_NAME})"

    def exclude_by_sequence_length(self, min_length=0):
        """Filter metadata by sequence length from a given sequence index.

        Parameters
        ----------
        min_length : int
            Minimum number of standard nucleotide characters (A, C, G, or T) in each sequence

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        return f"""{STRAIN_COL} IN (
            SELECT {STRAIN_COL}
            FROM {SEQUENCE_INDEX_TABLE_NAME}
            WHERE A+C+G+T < {min_length}
        )"""

    def exclude_by_non_nucleotide(self):
        """Filter metadata for strains with invalid nucleotide content.

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        return f"""{STRAIN_COL} IN (
            SELECT {STRAIN_COL}
            FROM {SEQUENCE_INDEX_TABLE_NAME}
            WHERE invalid_nucleotides != 0
        )"""

    def force_include_strains(self, include_file):
        """Include strains in the given text file from the given metadata.

        Parameters
        ----------
        include_file : str
            Filename with strain names to include from the given metadata

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        included_strains = read_strains(include_file)
        included_strains = [f"'{strain}'" for strain in included_strains]
        return f"{STRAIN_COL} IN ({','.join(included_strains)})"

    def force_include_where(self, include_where):
        """Include all strains from the given metadata that match the given query.

        Unlike pandas query syntax, inclusion queries should follow the pattern of
        `"property=value"` or `"property!=value"`. Additionally, this filter treats
        all values like lowercase strings, so we convert all values to strings first
        and then lowercase them before testing the given query.

        Parameters
        ----------
        include_where : str
            Filter query used to include strains

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        column, op, value = self.parse_filter_query(include_where)
        return f"""{STRAIN_COL} IN (
            SELECT {STRAIN_COL}
            FROM {METADATA_TABLE_NAME}
            WHERE {column} {op} '{value}'
        )
        """

    def db_create_filtered_view(self, exclude_by:list, include_by:list):
        self.db_apply_filters(exclude_by, include_by)

    def db_apply_filters(self, exclude_by:list, include_by:list):
        """Apply a list of filters to exclude or force-include records from the given
        metadata and return the strains to keep, to exclude, and to force include.

        Parameters
        ----------
        exclude_by : list
            A list of filter expressions for duckdb.filter.
        include_by : list
            A list of filter expressions for duckdb.filter.
        Returns
        -------
        DuckDBPyRelation
            relation for filtered metadata
        """
        self.db_create_filter_reason_table()
        self.db_apply_exclusions(exclude_by)
        self.db_apply_force_inclusions(include_by)
        self.db_create_filtered_table()

    def db_create_filter_reason_table(self):
        self.cur.execute(f"DROP TABLE IF EXISTS {METADATA_FILTER_REASON_TABLE_NAME}")
        self.cur.execute(f"""
            CREATE TABLE {METADATA_FILTER_REASON_TABLE_NAME} AS
            SELECT
                {STRAIN_COL},
                FALSE as {EXCLUDE_COL},
                FALSE as {INCLUDE_COL},
                NULL as {FILTER_REASON_COL}
            FROM {METADATA_TABLE_NAME}
        """)
        self.cur.execute(f"CREATE UNIQUE INDEX idx_{METADATA_FILTER_REASON_TABLE_NAME}_{STRAIN_COL} ON {METADATA_FILTER_REASON_TABLE_NAME} ({STRAIN_COL})")

    def db_apply_exclusions(self, exclude_by):
        for exclude_rule_name, where_filter in exclude_by:
            self.cur.execute(f"""
                UPDATE {METADATA_FILTER_REASON_TABLE_NAME}
                SET
                    {EXCLUDE_COL} = TRUE,
                    {FILTER_REASON_COL} = '{exclude_rule_name}'
                WHERE {where_filter}
            """)
            self.connection.commit()

    def db_apply_force_inclusions(self, include_by):
        for include_rule_name, where_filter in include_by:
            self.cur.execute(f"""
                UPDATE {METADATA_FILTER_REASON_TABLE_NAME}
                SET
                    {INCLUDE_COL} = TRUE,
                    {FILTER_REASON_COL} = '{include_rule_name}'
                WHERE {where_filter}
            """)
            self.connection.commit()

    def db_create_filtered_table(self):
        self.cur.execute(f"DROP TABLE IF EXISTS {FILTERED_TABLE_NAME}")
        self.cur.execute(f"""
            CREATE TABLE {FILTERED_TABLE_NAME} AS
            SELECT m.* FROM {METADATA_TABLE_NAME} m
            JOIN {METADATA_FILTER_REASON_TABLE_NAME} f
                USING ({STRAIN_COL})
            WHERE NOT f.{EXCLUDE_COL} OR f.{INCLUDE_COL}
        """)

    def db_create_output_table(self, input_table:str):
        self.cur.execute(f"DROP TABLE IF EXISTS {OUTPUT_METADATA_TABLE_NAME}")
        self.cur.execute(f"CREATE TABLE {OUTPUT_METADATA_TABLE_NAME} AS SELECT * FROM {input_table}")

    def db_get_counts_per_group(self, group_by_cols:List[str]):
        self.cur.execute(f"""
            SELECT {','.join(group_by_cols)}, COUNT(*)
            FROM {EXTENDED_VIEW_NAME}
            GROUP BY {','.join(group_by_cols)}
        """)
        return [row[-1] for row in self.cur.fetchall()]

    def db_get_filtered_strains_count(self):
        self.cur.execute(f"SELECT COUNT(*) FROM {FILTERED_TABLE_NAME}")
        return self.cur.fetchone()[0]

    def db_create_subsampled_table(self, group_by_cols:List[str]):
        # create a view for subsampled strains
        where_conditions = [f'group_i <= {GROUP_SIZE_COL}']
        for col in group_by_cols:
            where_conditions.append(f'{col} IS NOT NULL')
        query = f"""
            SELECT {STRAIN_COL}
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {','.join(group_by_cols)}
                    ORDER BY {PRIORITY_COL} DESC NULLS LAST
                ) AS group_i
                FROM {EXTENDED_VIEW_NAME}
            )
            JOIN {GROUP_SIZES_TABLE_NAME} USING({','.join(group_by_cols)})
            WHERE {' AND '.join(where_conditions)}
        """
        self.cur.execute(f"DROP VIEW IF EXISTS {SUBSAMPLE_STRAINS_VIEW_NAME}")
        self.cur.execute(f"CREATE VIEW {SUBSAMPLE_STRAINS_VIEW_NAME} AS {query}")
        # use subsample strains to select rows from filtered metadata
        self.connection.execute(f"DROP TABLE IF EXISTS {SUBSAMPLED_TABLE_NAME}")
        df_subsampled = pd.read_sql_query(f"""
            SELECT * FROM {FILTERED_TABLE_NAME}
            WHERE {STRAIN_COL} IN (SELECT {STRAIN_COL} FROM {SUBSAMPLE_STRAINS_VIEW_NAME})
        """, self.connection)
        df_subsampled.to_sql(SUBSAMPLED_TABLE_NAME, self.connection)

    def db_load_priorities_table(self):
        load_tsv(self.connection, self.args.priority, PRIORITIES_TABLE_NAME, header=False, names=[STRAIN_COL, PRIORITY_COL])

    def db_generate_priorities_table(self, seed:int=None):
        # TODO: seed... might not be possible https://stackoverflow.com/a/24394275
        self.cur.execute(f"DROP TABLE IF EXISTS {PRIORITIES_TABLE_NAME}")
        self.cur.execute(f"""
            CREATE TABLE {PRIORITIES_TABLE_NAME} AS
            SELECT {STRAIN_COL}, RANDOM() AS {PRIORITY_COL}
            FROM {FILTERED_TABLE_NAME}
        """)

    def db_create_extended_filtered_metadata_view(self):
        # create new view that extends strain with year/month/day, priority, dummy
        self.cur.execute(f"DROP VIEW IF EXISTS {EXTENDED_VIEW_NAME}")
        self.cur.execute(f"""
        CREATE VIEW {EXTENDED_VIEW_NAME} AS
            SELECT m.*, d.year, d.month, d.day, p.{PRIORITY_COL}, TRUE AS {DUMMY_COL}
            FROM {FILTERED_TABLE_NAME} m
            JOIN {DATE_TABLE_NAME} d ON m.{STRAIN_COL} = d.{STRAIN_COL}
            LEFT OUTER JOIN {PRIORITIES_TABLE_NAME} p ON m.{STRAIN_COL} = p.{STRAIN_COL}
        """)

    def db_create_group_sizes_table(self, group_by:list, sequences_per_group:float):
        df_groups = pd.read_sql_query(f"""
                SELECT {','.join(group_by)}
                FROM {EXTENDED_VIEW_NAME}
                GROUP BY {','.join(group_by)}
            """, self.connection)
        df_sizes = get_sizes_per_group(df_groups, GROUP_SIZE_COL, sequences_per_group, random_seed=self.args.subsample_seed)
        self.cur.execute(f"DROP TABLE IF EXISTS {GROUP_SIZES_TABLE_NAME}")
        df_sizes.to_sql(GROUP_SIZES_TABLE_NAME, self.connection)

    def db_output_strains(self):
        df = pd.read_sql_query(f"SELECT {STRAIN_COL} FROM {OUTPUT_METADATA_TABLE_NAME}", self.connection)
        df.to_csv(self.args.output_strains, index=None, header=False)

    def db_output_metadata(self):
        df = pd.read_sql_query(f"SELECT * FROM {OUTPUT_METADATA_TABLE_NAME}", self.connection)
        df.to_csv(self.args.output_metadata, sep='\t', index=None)


def get_year(date:str):
    try:
        return int(date.split('-')[0])
    except:
        return None


def get_month(date:str):
    try:
        return int(date.split('-')[1])
    except:
        return None


def get_day(date:str):
    try:
        return int(date.split('-')[2])
    except:
        return None


def get_date_min(date:str):
    # TODO: check month/day value boundaries
    if not date:
        return None
    date_parts = date.split('-', maxsplit=2)
    year = date_parts[0]
    month = date_parts[1] if len(date_parts) > 1 and date_parts[1].isnumeric() else '01'
    day = date_parts[2] if len(date_parts) > 2 and date_parts[2].isnumeric() else '01'
    return f'{year}-{month}-{day}'


def get_date_max(date:str):
    # TODO: check month/day value boundaries
    if not date:
        return None
    date_parts = date.split('-', maxsplit=2)
    year = date_parts[0]
    month = date_parts[1] if len(date_parts) > 1 and date_parts[1].isnumeric() else '12'
    if len(date_parts) == 3 and date_parts[2].isnumeric():
        day = date_parts[2]
    else:
        month_num = int(month)
        if month_num in {1,3,5,7,8,10,12}:
            day = '31'
        elif month_num == 2:
            day = '28'
        else:
            day = '30'
    return f'{year}-{month}-{day}'
