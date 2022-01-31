import re
from typing import List
import numpy as np
import pandas as pd
import sqlite3
import argparse
from datetime import date

from augur.io_support.db.sqlite import load_tsv, cleanup, ROW_ORDER_COLUMN
from augur.utils import read_strains
from .base import FilterBase
from augur.filter_support.subsample import get_sizes_per_group
from augur.filter_support.output import filter_kwargs_to_str


DEFAULT_DB_FILE = 'test.sqlite3'

METADATA_TABLE_NAME = 'metadata'
SEQUENCE_INDEX_TABLE_NAME = 'sequence_index'
PRIORITIES_TABLE_NAME = 'priorities'
DATE_TABLE_NAME = 'metadata_date_expanded'
METADATA_FILTER_REASON_TABLE_NAME = 'metadata_filtered_reason'
GROUP_SIZES_TABLE_NAME = 'group_sizes'
OUTPUT_METADATA_TABLE_NAME = 'metadata_output'

EXTENDED_VIEW_NAME = 'metadata_filtered_extended'

DEFAULT_DATE_COL = 'date'
FILTER_REASON_COL = 'filter'
FILTER_REASON_KWARGS_COL = 'kwargs'
EXCLUDE_COL = 'exclude'
INCLUDE_COL = 'force_include'
DUMMY_COL = 'dummy'
GROUP_SIZE_COL = 'size'
STRAIN_COL = 'strain'
PRIORITY_COL = 'priority'

SUBSAMPLE_FILTER_REASON = 'subsampling'

# TODO: parameterize
N_JOBS = 4

class FilterSQLite(FilterBase):
    def __init__(self, args:argparse.Namespace):
        super().__init__(args)

    def db_connect(self, database:str=DEFAULT_DB_FILE):
        self.connection = sqlite3.connect(database)
        self.cur = self.connection.cursor()

    def db_create_strain_index(self, table_name:str):
        self.cur.execute(f"""
            CREATE UNIQUE INDEX idx_{table_name}_{STRAIN_COL}
            ON {table_name} ({STRAIN_COL})
        """)

    def db_load_metadata(self, database:str=DEFAULT_DB_FILE):
        load_tsv(self.args.metadata, database, METADATA_TABLE_NAME, n_jobs=N_JOBS)
        self.db_create_strain_index(METADATA_TABLE_NAME)

    def db_load_sequence_index(self, path, database:str=DEFAULT_DB_FILE):
        load_tsv(path, database, SEQUENCE_INDEX_TABLE_NAME, n_jobs=N_JOBS)
        self.db_create_strain_index(SEQUENCE_INDEX_TABLE_NAME)

    def db_get_sequence_index_strains(self):
        self.cur.execute(f"""
            SELECT {STRAIN_COL}
            FROM {SEQUENCE_INDEX_TABLE_NAME}
        """)
        return {row[0] for row in self.cur.fetchall()}

    def db_has_date_col(self):
        columns = {i[1] for i in self.cur.execute(f'PRAGMA table_info({METADATA_TABLE_NAME})')}
        return (DEFAULT_DATE_COL in columns)

    def db_create_date_table(self):
        self.connection.create_function(get_year.__name__, 1, get_year)
        self.connection.create_function(get_month.__name__, 1, get_month)
        self.connection.create_function(get_day.__name__, 1, get_day)
        self.connection.create_function(get_date_min.__name__, 1, get_date_min)
        self.connection.create_function(get_date_max.__name__, 1, get_date_max)
        self.cur.execute(f"""CREATE TABLE {DATE_TABLE_NAME} AS
            SELECT
                {STRAIN_COL},
                {DEFAULT_DATE_COL},
                {get_year.__name__}({DEFAULT_DATE_COL}) as year,
                {get_month.__name__}({DEFAULT_DATE_COL}) as month,
                {get_day.__name__}({DEFAULT_DATE_COL}) as day,
                {get_date_min.__name__}({DEFAULT_DATE_COL}) as date_min,
                {get_date_max.__name__}({DEFAULT_DATE_COL}) as date_max
            FROM {METADATA_TABLE_NAME}
        """)
        self.db_create_strain_index(DATE_TABLE_NAME)

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

    def filter_by_exclude_strains(self, exclude_file):
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

    def filter_by_exclude_where(self, exclude_where):
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

    def filter_by_ambiguous_date(self, ambiguity="any"):
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

    def filter_by_min_date(self, min_date):
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
        min_date = get_date_min(min_date)
        return f"""{STRAIN_COL} IN (
            SELECT {STRAIN_COL}
            FROM {DATE_TABLE_NAME}
            WHERE date_min < {min_date} OR date_min IS NULL
        )"""

    def filter_by_max_date(self, max_date):
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
        max_date = get_date_max(max_date)
        return f"""{STRAIN_COL} IN (
            SELECT {STRAIN_COL}
            FROM {DATE_TABLE_NAME}
            WHERE date_max > {max_date} OR date_max IS NULL
        )"""

    def filter_by_sequence_index(self):
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

    def filter_by_sequence_length(self, min_length=0):
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

    def filter_by_non_nucleotide(self):
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

    def db_create_filter_reason_table(self, exclude_by:list, include_by:list):
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
        self.cur.execute(f"""
            CREATE TABLE {METADATA_FILTER_REASON_TABLE_NAME} AS
            SELECT
                {STRAIN_COL},
                FALSE as {EXCLUDE_COL},
                FALSE as {INCLUDE_COL},
                NULL as {FILTER_REASON_COL},
                NULL as {FILTER_REASON_KWARGS_COL}
            FROM {METADATA_TABLE_NAME}
        """)
        self.db_create_strain_index(METADATA_FILTER_REASON_TABLE_NAME)
        self.db_apply_exclusions(exclude_by)
        self.db_apply_force_inclusions(include_by)

    def db_apply_exclusions(self, exclude_by):
        for exclude_function, kwargs in exclude_by:
            where_filter = exclude_function(**kwargs)
            kwargs_str = filter_kwargs_to_str(kwargs)
            kwargs_str = kwargs_str.replace('\'', '\'\'') # escape single quote for SQLite
            self.cur.execute(f"""
                UPDATE {METADATA_FILTER_REASON_TABLE_NAME}
                SET
                    {EXCLUDE_COL} = TRUE,
                    {FILTER_REASON_COL} = '{exclude_function.__name__}',
                    {FILTER_REASON_KWARGS_COL} = '{kwargs_str}'
                WHERE {where_filter}
            """)
            self.connection.commit()

    def db_apply_force_inclusions(self, include_by):
        for include_function, kwargs in include_by:
            where_filter = include_function(**kwargs)
            kwargs_str = filter_kwargs_to_str(kwargs)
            kwargs_str = kwargs_str.replace('\'', '\'\'') # escape single quote for SQLite
            self.cur.execute(f"""
                UPDATE {METADATA_FILTER_REASON_TABLE_NAME}
                SET
                    {INCLUDE_COL} = TRUE,
                    {FILTER_REASON_COL} = '{include_function.__name__}',
                    {FILTER_REASON_KWARGS_COL} = '{kwargs_str}'
                WHERE {where_filter}
            """)
            self.connection.commit()

    def db_create_output_table(self):
        self.cur.execute(f"""
            CREATE TABLE {OUTPUT_METADATA_TABLE_NAME} AS
            SELECT m.* FROM {METADATA_TABLE_NAME} m
            JOIN {METADATA_FILTER_REASON_TABLE_NAME} f
                USING ({STRAIN_COL})
            WHERE NOT f.{EXCLUDE_COL} OR f.{INCLUDE_COL}
        """)

    def db_get_counts_per_group(self, group_by_cols:List[str]):
        self.cur.execute(f"""
            SELECT {','.join(group_by_cols)}, COUNT(*)
            FROM {EXTENDED_VIEW_NAME}
            GROUP BY {','.join(group_by_cols)}
        """)
        return [row[-1] for row in self.cur.fetchall()]

    def db_get_filtered_strains_count(self):
        self.cur.execute(f"""
            SELECT COUNT(*)
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        return self.cur.fetchone()[0]

    def db_update_filter_reason_table_with_subsampling(self, group_by_cols:List[str]):
        # create a SQL query for strains to subsample for
        where_conditions = [f'group_i <= {GROUP_SIZE_COL}']
        for col in group_by_cols:
            where_conditions.append(f'{col} IS NOT NULL')
        # `ORDER BY ... NULLS LAST` is unsupported for SQLite <3.30.0 so `CASE ... IS NULL` is a workaround
        # ref https://stackoverflow.com/a/12503284
        query_for_subsampled_strains = f"""
            SELECT {STRAIN_COL}
            FROM (
                SELECT {STRAIN_COL}, {','.join(group_by_cols)}, ROW_NUMBER() OVER (
                    PARTITION BY {','.join(group_by_cols)}
                    ORDER BY (CASE WHEN {PRIORITY_COL} IS NULL THEN 1 ELSE 0 END), {PRIORITY_COL} DESC
                ) AS group_i
                FROM {EXTENDED_VIEW_NAME}
            )
            JOIN {GROUP_SIZES_TABLE_NAME} USING({','.join(group_by_cols)})
            WHERE {' AND '.join(where_conditions)}
        """
        # update filter reason table
        self.cur.execute(f"""
            UPDATE {METADATA_FILTER_REASON_TABLE_NAME}
            SET
                {EXCLUDE_COL} = TRUE,
                {FILTER_REASON_COL} = '{SUBSAMPLE_FILTER_REASON}'
            WHERE NOT {EXCLUDE_COL} AND {STRAIN_COL} NOT IN (
                {query_for_subsampled_strains}
            )
        """)
        self.connection.commit()

    def db_load_priorities_table(self, database:str=DEFAULT_DB_FILE):
        dtype = {
            STRAIN_COL: 'str',
            PRIORITY_COL: 'float'
        }
        try:
            load_tsv(self.args.priority, database, PRIORITIES_TABLE_NAME,
                    header=False, names=[STRAIN_COL, PRIORITY_COL], dtype=dtype,
                    n_jobs=N_JOBS)
        except ValueError as e:
            raise ValueError("Failed to parse priority file.") from e
        self.db_create_strain_index(PRIORITIES_TABLE_NAME)

    def db_generate_priorities_table(self, seed:int=None):
        # use pandas/numpy since random seeding is not possible with SQLite https://stackoverflow.com/a/24394275
        df_priority = pd.read_sql(f"""
                SELECT {STRAIN_COL}
                FROM {METADATA_FILTER_REASON_TABLE_NAME}
                WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
            """, self.connection)
        rng = np.random.default_rng(seed)
        df_priority[PRIORITY_COL] = rng.random(len(df_priority))
        df_priority.to_sql(PRIORITIES_TABLE_NAME, self.connection, index=False)
        self.db_create_strain_index(PRIORITIES_TABLE_NAME)

    def db_create_extended_filtered_metadata_view(self):
        # create new view that extends strain with year/month/day, priority, dummy
        self.cur.execute(f"DROP VIEW IF EXISTS {EXTENDED_VIEW_NAME}")
        self.cur.execute(f"""
        CREATE VIEW {EXTENDED_VIEW_NAME} AS
            SELECT m.*, d.year, d.month, d.day, p.{PRIORITY_COL}, TRUE AS {DUMMY_COL}
            FROM {METADATA_TABLE_NAME} m
            JOIN {METADATA_FILTER_REASON_TABLE_NAME} f ON (m.{STRAIN_COL} = f.{STRAIN_COL})
                AND (NOT f.{EXCLUDE_COL} OR f.{INCLUDE_COL})
            JOIN {DATE_TABLE_NAME} d USING ({STRAIN_COL})
            LEFT OUTER JOIN {PRIORITIES_TABLE_NAME} p USING ({STRAIN_COL})
        """)

    def db_create_group_sizes_table(self, group_by:list, sequences_per_group:float):
        df_groups = pd.read_sql_query(f"""
                SELECT {','.join(group_by)}
                FROM {EXTENDED_VIEW_NAME}
                GROUP BY {','.join(group_by)}
            """, self.connection)
        df_sizes = get_sizes_per_group(df_groups, GROUP_SIZE_COL, sequences_per_group, random_seed=self.args.subsample_seed)
        df_sizes.to_sql(GROUP_SIZES_TABLE_NAME, self.connection)

    def db_output_strains(self):
        df = pd.read_sql_query(f"SELECT {STRAIN_COL} FROM {OUTPUT_METADATA_TABLE_NAME} ORDER BY {ROW_ORDER_COLUMN}", self.connection)
        df.to_csv(self.args.output_strains, index=None, header=False)

    def db_output_metadata(self):
        df = pd.read_sql_query(f"SELECT * FROM {OUTPUT_METADATA_TABLE_NAME} ORDER BY {ROW_ORDER_COLUMN}", self.connection)
        df.drop(ROW_ORDER_COLUMN, axis=1, inplace=True)
        df.to_csv(self.args.output_metadata, sep='\t', index=None)

    def db_output_log(self):
        df = pd.read_sql_query(f"""
                SELECT {STRAIN_COL}, {FILTER_REASON_COL}, {FILTER_REASON_KWARGS_COL}
                FROM {METADATA_FILTER_REASON_TABLE_NAME}
                WHERE {FILTER_REASON_COL} IS NOT NULL
            """, self.connection)
        df.to_csv(self.args.output_log, sep='\t', index=None)

    def db_get_metadata_strains(self):
        self.cur.execute(f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_TABLE_NAME}
        """)
        return {row[0] for row in self.cur.fetchall()}

    def db_get_strains_passed(self):
        self.cur.execute(f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        return {row[0] for row in self.cur.fetchall()}

    def db_get_strains_passed_count(self):
        self.cur.execute(f"""
            SELECT COUNT(*)
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        return self.cur.fetchone()[0]

    def db_get_num_metadata_strains(self):
        self.cur.execute(f"""
            SELECT COUNT(*)
            FROM {METADATA_TABLE_NAME}
        """)
        return self.cur.fetchone()[0]

    def db_get_num_excluded_subsamp(self):
        self.cur.execute(f"""
            SELECT COUNT(*)
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = '{SUBSAMPLE_FILTER_REASON}'
        """)
        return self.cur.fetchone()[0]

    def db_get_filter_counts(self):
        self.cur.execute(f"""
            SELECT {FILTER_REASON_COL}, {FILTER_REASON_KWARGS_COL}, COUNT(*)
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} IS NOT NULL
                AND {FILTER_REASON_COL} != '{SUBSAMPLE_FILTER_REASON}'
            GROUP BY {FILTER_REASON_COL}, {FILTER_REASON_KWARGS_COL}
        """)
        return self.cur.fetchall()

    def db_cleanup(self, database:str=DEFAULT_DB_FILE):
        cleanup(database)


def get_year(date_in:str):
    try:
        return int(date_in.split('-')[0])
    except:
        return None


def get_month(date_in:str):
    try:
        return int(date_in.split('-')[1])
    except:
        return None


def get_day(date_in:str):
    try:
        return int(date_in.split('-')[2])
    except:
        return None


# TODO: DateDisambiguator parity
# assert_only_less_significant_uncertainty
# max_date = min(max_date, datetime.date.today())

def get_date_min(date_in:str):
    if not date_in:
        return None
    if date_in.lstrip('-').isnumeric() and '.' in date_in:
        # date is a numeric date
        # can be negative
        # year-only is ambiguous
        return float(date_in)
    # convert to numeric
    # TODO: check month/day value boundaries
    # TODO: raise exception for negative ISO dates
    date_parts = date_in.split('-', maxsplit=2)
    year = int(date_parts[0].replace('X', '0'))
    month = int(date_parts[1]) if len(date_parts) > 1 and date_parts[1].isnumeric() else 1
    day = int(date_parts[2]) if len(date_parts) > 2 and date_parts[2].isnumeric() else 1
    return date_to_numeric(date(year, month, day))


def get_date_max(date_in:str):
    if not date_in:
        return None
    if date_in.lstrip('-').isnumeric() and '.' in date_in:
        # date is a numeric date
        # can be negative
        # year-only is ambiguous
        return float(date_in)
    # convert to numeric
    # TODO: check month/day value boundaries
    # TODO: raise exception for negative ISO dates
    date_parts = date_in.split('-', maxsplit=2)
    year = int(date_parts[0].replace('X', '9'))
    month = int(date_parts[1]) if len(date_parts) > 1 and date_parts[1].isnumeric() else 12
    if len(date_parts) == 3 and date_parts[2].isnumeric():
        day = int(date_parts[2])
    else:
        if month in {1,3,5,7,8,10,12}:
            day = 31
        elif month == 2:
            day = 28
        else:
            day = 30
    return date_to_numeric(date(year, month, day))


# copied from treetime.utils.numeric_date
# simplified+cached for speed
from calendar import isleap
date_to_numeric_cache = dict()
def date_to_numeric(d:date):
    if d not in date_to_numeric_cache:
        days_in_year = 366 if isleap(d.year) else 365
        numeric_date = d.year + (d.timetuple().tm_yday-0.5) / days_in_year
        date_to_numeric_cache[d] = numeric_date
    return date_to_numeric_cache[d]
