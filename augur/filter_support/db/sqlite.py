import re
from typing import List, Set, Tuple
import numpy as np
import pandas as pd
import sqlite3
from tempfile import NamedTemporaryFile

from augur.io_support.db.sqlite import load_tsv, cleanup, ROW_ORDER_COLUMN
from augur.utils import read_strains
from augur.filter_support.db.base import FilterBase
from augur.filter_support.date_parsing import InvalidDateFormat, get_year, get_month, get_day, get_date_min, get_date_max
from augur.filter_support.subsample import get_sizes_per_group
from augur.filter_support.output import filter_kwargs_to_str

# internal database globals
# table names
METADATA_TABLE_NAME = 'metadata'
SEQUENCE_INDEX_TABLE_NAME = 'sequence_index'
PRIORITIES_TABLE_NAME = 'priorities'
DATE_TABLE_NAME = 'metadata_date_expanded'
METADATA_FILTER_REASON_TABLE_NAME = 'metadata_filtered_reason'
EXTENDED_FILTERED_TABLE_NAME = 'metadata_filtered_extended'
GROUP_SIZES_TABLE_NAME = 'group_sizes'
OUTPUT_METADATA_TABLE_NAME = 'metadata_output'
# column names
DATE_YEAR_COL = 'year'
DATE_MONTH_COL = 'month'
DATE_DAY_COL = 'day'
DATE_MIN_COL = 'date_min'
DATE_MAX_COL = 'date_max'
FILTER_REASON_COL = 'filter'
FILTER_REASON_KWARGS_COL = 'kwargs'
EXCLUDE_COL = 'exclude'
INCLUDE_COL = 'force_include'
DUMMY_COL = 'dummy'
GROUP_SIZE_COL = 'size'
PRIORITY_COL = 'priority'
# value for FILTER_REASON_COL with separate logic
SUBSAMPLE_FILTER_REASON = 'subsampling'


class FilterSQLite(FilterBase):
    def __init__(self, db_file:str=''):
        if not db_file:
            tmp_file = NamedTemporaryFile(delete=False)
            db_file = tmp_file.name
        self.db_file = db_file

    def db_connect(self):
        """Creates a sqlite3 connection and cursor, stored as instance attributes."""
        self.connection = sqlite3.connect(self.db_file, timeout=15) # to reduce OperationalError: database is locked
        self.cur = self.connection.cursor()

    def db_create_strain_index(self, table_name:str):
        """Creates a unique index on `metadata_id_column` in a table."""
        self.cur.execute(f"""
            CREATE UNIQUE INDEX idx_{table_name}_strain_col
            ON {table_name} ("{self.metadata_id_column}")
        """)

    def db_load_metadata(self):
        """Loads a metadata file into the database.

        Retrieves the filename from `self.args`.
        """
        load_tsv(self.args.metadata, self.connection, METADATA_TABLE_NAME)
        self.db_create_strain_index(METADATA_TABLE_NAME)

    def db_load_sequence_index(self, path:str):
        """Loads a sequence index file into the database.

        Retrieves the filename from `self.args`.
        """
        load_tsv(path, self.connection, SEQUENCE_INDEX_TABLE_NAME)
        self.db_create_strain_index(SEQUENCE_INDEX_TABLE_NAME)

    def db_get_sequence_index_strains(self):
        """Returns the set of all strains in the sequence index."""
        self.cur.execute(f"""
            SELECT "{self.metadata_id_column}"
            FROM {SEQUENCE_INDEX_TABLE_NAME}
        """)
        return {row[0] for row in self.cur.fetchall()}

    def db_has_date_col(self):
        """Returns a boolean indicating whether `self.date_column` is in the metadata."""
        columns = {i[1] for i in self.cur.execute(f'PRAGMA table_info({METADATA_TABLE_NAME})')}
        return (self.date_column in columns)

    def db_create_date_table(self):
        """Creates an intermediate date table from the metadata table.

        Contains the strain column, original date column, and these computed columns:
        - `DATE_YEAR_COL`: Extracted year (int or `NULL`)
        - `DATE_MONTH_COL`: Extracted month (int or `NULL`)
        - `DATE_DAY_COL`: Extracted day (int or `NULL`)
        - `DATE_MIN_COL`: Exact date, minimum if ambiguous (`float` numeric date)
        - `DATE_MAX_COL`: Exact date, maximum if ambiguous (`float` numeric date)
        """
        if self.has_date_col:
            # TODO: handle numeric dates for year/month/day
            self.connection.create_function(get_year.__name__, 1, get_year)
            self.connection.create_function(get_month.__name__, 1, get_month)
            self.connection.create_function(get_day.__name__, 1, get_day)
            self.connection.create_function(get_date_min.__name__, 1, get_date_min)
            self.connection.create_function(get_date_max.__name__, 1, get_date_max)
            self.cur.execute(f"""CREATE TABLE {DATE_TABLE_NAME} AS
                SELECT
                    "{self.metadata_id_column}",
                    "{self.date_column}",
                    {get_year.__name__}("{self.date_column}") as {DATE_YEAR_COL},
                    {get_month.__name__}("{self.date_column}") as {DATE_MONTH_COL},
                    {get_day.__name__}("{self.date_column}") as {DATE_DAY_COL},
                    {get_date_min.__name__}("{self.date_column}") as {DATE_MIN_COL},
                    {get_date_max.__name__}("{self.date_column}") as {DATE_MAX_COL}
                FROM {METADATA_TABLE_NAME}
            """)
            # skip validation, but implemented if needed in the future
            # self._validate_date_table()
        else:
            # create placeholder table for later JOINs
            self.cur.execute(f"""CREATE TABLE {DATE_TABLE_NAME} AS
                SELECT
                    "{self.metadata_id_column}",
                    '' as {DATE_YEAR_COL},
                    '' as {DATE_MONTH_COL},
                    '' as {DATE_DAY_COL},
                    '' as {DATE_MIN_COL},
                    '' as {DATE_MAX_COL}
                FROM {METADATA_TABLE_NAME}
            """)
        self.db_create_strain_index(DATE_TABLE_NAME)

    def _validate_date_table(self):
        """Validate dates in `DATE_TABLE_NAME`.

        Internally runs a query for invalid dates, i.e. rows where:
        1. date was specified (not null or empty string)
        2. min/max date could not be determined (null value)

        Raises
        ------
        :class:`InvalidDateFormat`
        """
        max_results = 3 # limit length of error message
        self.cur.execute(f"""
            SELECT cast("{self.date_column}" as text)
            FROM {DATE_TABLE_NAME}
            WHERE NOT ("{self.date_column}" IS NULL OR "{self.date_column}" = '')
                AND ({DATE_MIN_COL} IS NULL OR {DATE_MAX_COL} IS NULL)
            LIMIT {max_results}
        """)
        invalid_dates = [repr(row[0]) for row in self.cur.fetchall()]
        if invalid_dates:
            raise InvalidDateFormat(f"Some dates have an invalid format (showing at most {max_results}): {','.join(invalid_dates)}")

    def filter_by_exclude_all(self):
        """Exclude all strains regardless of the given metadata content.

        This is a placeholder function that can be called as part of a generalized
        loop through all possible functions.

        Returns
        -------
        str:
            expression for SQL query `WHERE` clause
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
            expression for SQL query `WHERE` clause
        """
        excluded_strains = read_strains(exclude_file)
        excluded_strains = [f"'{strain}'" for strain in excluded_strains]
        return f"""
            "{self.metadata_id_column}" IN ({','.join(excluded_strains)})
        """

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

        >>> FilterSQLite.parse_filter_query(FilterSQLite, "property=value")
        ('property', '=', 'value')
        >>> FilterSQLite.parse_filter_query(FilterSQLite, "property!=value")
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
            expression for SQL query `WHERE` clause
        """
        column, op, value = self.parse_filter_query(exclude_where)
        return f"""
            "{self.metadata_id_column}" IN (
                SELECT "{self.metadata_id_column}"
                FROM {METADATA_TABLE_NAME}
                WHERE {column} {op} '{value}'
            )
        """

    def filter_by_query(self, query):
        """Filter by any valid SQL expression on the metadata.

        Strains that do *not* match the query will be excluded.

        Parameters
        ----------
        query : str
            SQL expression used to exclude strains

        Returns
        -------
        str:
            expression for SQL query `WHERE` clause
        """
        # NOT query to exclude all that do not match
        return f"""
            "{self.metadata_id_column}" IN (
                SELECT "{self.metadata_id_column}"
                FROM {METADATA_TABLE_NAME}
                WHERE NOT ({query})
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
            expression for SQL query `WHERE` clause
        """
        if ambiguity == 'year':
            return f"""
                "{self.metadata_id_column}" IN (
                    SELECT "{self.metadata_id_column}"
                    FROM {DATE_TABLE_NAME}
                    WHERE {DATE_YEAR_COL} IS NULL
                )
            """
        if ambiguity == 'month':
            return f"""
                "{self.metadata_id_column}" IN (
                    SELECT "{self.metadata_id_column}"
                    FROM {DATE_TABLE_NAME}
                    WHERE {DATE_MONTH_COL} IS NULL OR {DATE_YEAR_COL} IS NULL
                )
            """
        if ambiguity == 'day' or ambiguity == 'any':
            return f"""
                "{self.metadata_id_column}" IN (
                    SELECT "{self.metadata_id_column}"
                    FROM {DATE_TABLE_NAME}
                    WHERE {DATE_DAY_COL} IS NULL OR {DATE_MONTH_COL} IS NULL OR {DATE_YEAR_COL} IS NULL
                )
            """

    def filter_by_min_date(self, min_date):
        """Filter metadata by minimum date.

        Parameters
        ----------
        min_date : float
            Minimum date

        Returns
        -------
        str:
            expression for SQL query `WHERE` clause
        """
        min_date = get_date_min(min_date)
        return f"""
            "{self.metadata_id_column}" IN (
                SELECT "{self.metadata_id_column}"
                FROM {DATE_TABLE_NAME}
                WHERE {DATE_MAX_COL} < {min_date} OR {DATE_MIN_COL} IS NULL
            )
        """

    def filter_by_max_date(self, max_date):
        """Filter metadata by maximum date.

        Parameters
        ----------
        max_date : float
            Maximum date

        Returns
        -------
        str:
            expression for SQL query `WHERE` clause
        """
        max_date = get_date_max(max_date)
        return f"""
            "{self.metadata_id_column}" IN (
                SELECT "{self.metadata_id_column}"
                FROM {DATE_TABLE_NAME}
                WHERE {DATE_MIN_COL} > {max_date} OR {DATE_MAX_COL} IS NULL
            )
        """

    def filter_by_sequence_index(self):
        """Filter metadata by presence of corresponding entries in a given sequence
        index. This filter effectively intersects the strain ids in the metadata and
        sequence index.

        Returns
        -------
        str:
            expression for SQL query `WHERE` clause
        """
        return f"""
            "{self.metadata_id_column}" NOT IN (
                SELECT "{self.metadata_id_column}"
                FROM {SEQUENCE_INDEX_TABLE_NAME}
            )
        """

    def filter_by_sequence_length(self, min_length=0):
        """Filter metadata by sequence length from a given sequence index.

        Parameters
        ----------
        min_length : int
            Minimum number of standard nucleotide characters (A, C, G, or T) in each sequence

        Returns
        -------
        str:
            expression for SQL query `WHERE` clause
        """
        return f"""
            "{self.metadata_id_column}" IN (
                SELECT "{self.metadata_id_column}"
                FROM {SEQUENCE_INDEX_TABLE_NAME}
                WHERE A+C+G+T < {min_length}
            )
        """

    def filter_by_non_nucleotide(self):
        """Filter metadata for strains with invalid nucleotide content.

        Returns
        -------
        str:
            expression for SQL query `WHERE` clause
        """
        return f"""
            "{self.metadata_id_column}" IN (
                SELECT "{self.metadata_id_column}"
                FROM {SEQUENCE_INDEX_TABLE_NAME}
                WHERE invalid_nucleotides != 0
            )
        """

    def force_include_strains(self, include_file):
        """Include strains in the given text file from the given metadata.

        Parameters
        ----------
        include_file : str
            Filename with strain names to include from the given metadata

        Returns
        -------
        str:
            expression for SQL query `WHERE` clause
        """
        included_strains = read_strains(include_file)
        included_strains = [f"'{strain}'" for strain in included_strains]
        return f"""
            "{self.metadata_id_column}" IN ({','.join(included_strains)})
        """

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
            expression for SQL query `WHERE` clause
        """
        column, op, value = self.parse_filter_query(include_where)
        return f"""
            "{self.metadata_id_column}" IN (
                SELECT "{self.metadata_id_column}"
                FROM {METADATA_TABLE_NAME}
                WHERE {column} {op} '{value}'
            )
        """

    def db_create_filter_reason_table(self, exclude_by:list, include_by:list):
        """Creates an intermediate table for filter reason.
        
        Applies exclusion and force-inclusion rules to filter strains from the metadata.

        Parameters
        ----------
        exclude_by : list
            A list of filter expressions for SQL query `WHERE` clause
        include_by : list
            A list of filter expressions for SQL query `WHERE` clause
        """
        self.cur.execute(f"""
            CREATE TABLE {METADATA_FILTER_REASON_TABLE_NAME} AS
            SELECT
                "{self.metadata_id_column}",
                FALSE as {EXCLUDE_COL},
                FALSE as {INCLUDE_COL},
                NULL as {FILTER_REASON_COL},
                NULL as {FILTER_REASON_KWARGS_COL}
            FROM {METADATA_TABLE_NAME}
        """)
        self.db_create_strain_index(METADATA_FILTER_REASON_TABLE_NAME)
        # note: consider JOIN vs subquery if performance issues https://stackoverflow.com/q/3856164
        self.db_apply_exclusions(exclude_by)
        self.db_apply_force_inclusions(include_by)

    def db_apply_exclusions(self, exclude_by):
        """Updates the filter reason table with exclusion rules."""
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
        """Updates the filter reason table with force-inclusion rules."""
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
        """Creates a final intermediate table to be used for output.

        The table is a subset of the original metadata table containing rows that pass all:
        1. exclusion rules
        2. force-inclusion rules
        3. subsampling
        """
        self.cur.execute(f"""
            CREATE TABLE {OUTPUT_METADATA_TABLE_NAME} AS
            SELECT m.* FROM {METADATA_TABLE_NAME} m
            JOIN {METADATA_FILTER_REASON_TABLE_NAME} f
                USING ("{self.metadata_id_column}")
            WHERE NOT f.{EXCLUDE_COL} OR f.{INCLUDE_COL}
        """)

    def db_get_counts_per_group(self, group_by_cols:List[str]) -> List[int]:
        """
        Returns
        -------
        list[int]
            List of counts per group.
        """
        self.cur.execute(f"""
            SELECT {','.join(group_by_cols)}, COUNT(*)
            FROM {EXTENDED_FILTERED_TABLE_NAME}
            GROUP BY {','.join(group_by_cols)}
        """)
        return [row[-1] for row in self.cur.fetchall()]

    def db_get_filtered_strains_count(self) -> int:
        """Returns the number of metadata strains that pass all filter rules.

        Note: this can return a different number before and after subsampling.
        """
        self.cur.execute(f"""
            SELECT COUNT(*)
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        return self.cur.fetchone()[0]

    def db_update_filter_reason_table_with_subsampling(self, group_by_cols:List[str]):
        """Subsamples filtered metadata and updates the filter reason table."""
        # create a SQL query for strains to subsample for
        where_conditions = [f'group_i <= {GROUP_SIZE_COL}']
        for col in group_by_cols:
            where_conditions.append(f'{col} IS NOT NULL')
        # `ORDER BY ... NULLS LAST` is unsupported for SQLite <3.30.0 so `CASE ... IS NULL` is a workaround
        # ref https://stackoverflow.com/a/12503284
        query_for_subsampled_strains = f"""
            SELECT "{self.metadata_id_column}"
            FROM (
                SELECT "{self.metadata_id_column}", {','.join(group_by_cols)}, ROW_NUMBER() OVER (
                    PARTITION BY {','.join(group_by_cols)}
                    ORDER BY (CASE WHEN {PRIORITY_COL} IS NULL THEN 1 ELSE 0 END), {PRIORITY_COL} DESC
                ) AS group_i
                FROM {EXTENDED_FILTERED_TABLE_NAME}
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
            WHERE NOT {EXCLUDE_COL} AND "{self.metadata_id_column}" NOT IN (
                {query_for_subsampled_strains}
            )
        """)
        self.connection.commit()

    def db_load_priorities_table(self):
        dtypes = {
            self.metadata_id_column: 'TEXT',
            PRIORITY_COL: 'NUMERIC'
        }
        try:
            load_tsv(self.args.priority, self.connection, PRIORITIES_TABLE_NAME,
                    header=False, names=[self.metadata_id_column, PRIORITY_COL])
        except ValueError as e:
            raise ValueError(f"Failed to parse priority file {self.args.priority}.") from e
        self.db_create_strain_index(PRIORITIES_TABLE_NAME)

    def db_generate_priorities_table(self, seed:int=None):
        # use pandas/numpy since random seeding is not possible with SQLite https://stackoverflow.com/a/24394275
        df_priority = pd.read_sql(f"""
                SELECT "{self.metadata_id_column}"
                FROM {METADATA_FILTER_REASON_TABLE_NAME}
                WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
            """, self.connection)
        rng = np.random.default_rng(seed)
        df_priority[PRIORITY_COL] = rng.random(len(df_priority))
        df_priority.to_sql(PRIORITIES_TABLE_NAME, self.connection, index=False)
        self.db_create_strain_index(PRIORITIES_TABLE_NAME)

    def db_get_metadata_cols(self) -> Set[str]:
        return {i[1] for i in self.cur.execute(f'PRAGMA table_info({METADATA_TABLE_NAME})')}

    def db_create_extended_filtered_metadata_table(self, group_by_cols:List[str]):
        """Creates a new table with rows as filtered metadata, with the following columns:

        1. Strain
        2. Group-by columns
        2. Computed date columns (`DATE_YEAR_COL`, `DATE_MONTH_COL`, `DATE_DAY_COL`)
        3. `PRIORITY_COL`
        4. `dummy` containing the same value in all rows, used when no group-by columns are provided
        """
        group_by_cols_for_select = ''
        if group_by_cols:
            # copy to avoid modifying original list
            group_by_cols_copy = list(group_by_cols)
            # ignore computed date columns, those are added directly from the date table
            # TODO: call out that these columns will be ignored from original metadata if present
            if DATE_YEAR_COL in group_by_cols:
                group_by_cols_copy.remove(DATE_YEAR_COL)
            if DATE_MONTH_COL in group_by_cols:
                group_by_cols_copy.remove(DATE_MONTH_COL)
            if group_by_cols_copy:
                # prefix columns with m. as metadata table alias
                # add an extra comma for valid SQL
                group_by_cols_for_select = ','.join(f'm.{col}' for col in group_by_cols_copy) + ','
        # left outer join priorities table to prevent dropping strains without a priority
        self.cur.execute(f"""CREATE TABLE {EXTENDED_FILTERED_TABLE_NAME} AS
            SELECT
                m."{self.metadata_id_column}",
                {group_by_cols_for_select}
                d.{DATE_YEAR_COL}, d.{DATE_MONTH_COL},
                p.{PRIORITY_COL},
                TRUE AS {DUMMY_COL}
            FROM {METADATA_TABLE_NAME} m
            JOIN {METADATA_FILTER_REASON_TABLE_NAME} f ON (m."{self.metadata_id_column}" = f."{self.metadata_id_column}")
                AND (NOT f.{EXCLUDE_COL} OR f.{INCLUDE_COL})
            JOIN {DATE_TABLE_NAME} d USING ("{self.metadata_id_column}")
            LEFT OUTER JOIN {PRIORITIES_TABLE_NAME} p USING ("{self.metadata_id_column}")
        """)

    def db_create_group_sizes_table(self, group_by:list, sequences_per_group:float):
        df_groups = pd.read_sql_query(f"""
                SELECT {','.join(group_by)}
                FROM {EXTENDED_FILTERED_TABLE_NAME}
                GROUP BY {','.join(group_by)}
            """, self.connection)
        df_sizes = get_sizes_per_group(df_groups, GROUP_SIZE_COL, sequences_per_group, random_seed=self.args.subsample_seed)
        df_sizes.to_sql(GROUP_SIZES_TABLE_NAME, self.connection)

    def db_output_strains(self):
        df = pd.read_sql_query(f"""
                SELECT "{self.metadata_id_column}" FROM {OUTPUT_METADATA_TABLE_NAME} ORDER BY {ROW_ORDER_COLUMN}
            """, self.connection)
        df.to_csv(self.args.output_strains, index=None, header=False)

    def db_output_metadata(self):
        df = pd.read_sql_query(f"SELECT * FROM {OUTPUT_METADATA_TABLE_NAME} ORDER BY {ROW_ORDER_COLUMN}", self.connection)
        df.drop(ROW_ORDER_COLUMN, axis=1, inplace=True)
        df.to_csv(self.args.output_metadata, sep='\t', index=None)

    def db_output_log(self):
        df = pd.read_sql_query(f"""
                SELECT "{self.metadata_id_column}", {FILTER_REASON_COL}, {FILTER_REASON_KWARGS_COL}
                FROM {METADATA_FILTER_REASON_TABLE_NAME}
                WHERE {FILTER_REASON_COL} IS NOT NULL
            """, self.connection)
        df.to_csv(self.args.output_log, sep='\t', index=None)

    def db_get_metadata_strains(self) -> Set[str]:
        self.cur.execute(f"""
            SELECT "{self.metadata_id_column}"
            FROM {METADATA_TABLE_NAME}
        """)
        return {row[0] for row in self.cur.fetchall()}

    def db_get_strains_passed(self) -> Set[str]:
        self.cur.execute(f"""
            SELECT "{self.metadata_id_column}"
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        return {row[0] for row in self.cur.fetchall()}

    def db_get_num_metadata_strains(self) -> int:
        self.cur.execute(f"""
            SELECT COUNT(*)
            FROM {METADATA_TABLE_NAME}
        """)
        return self.cur.fetchone()[0]

    def db_get_num_excluded_subsamp(self) -> int:
        self.cur.execute(f"""
            SELECT COUNT(*)
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = '{SUBSAMPLE_FILTER_REASON}'
        """)
        return self.cur.fetchone()[0]

    def db_get_filter_counts(self) -> List[Tuple[str, str, int]]:
        self.cur.execute(f"""
            SELECT {FILTER_REASON_COL}, {FILTER_REASON_KWARGS_COL}, COUNT(*)
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} IS NOT NULL
                AND {FILTER_REASON_COL} != '{SUBSAMPLE_FILTER_REASON}'
            GROUP BY {FILTER_REASON_COL}, {FILTER_REASON_KWARGS_COL}
        """)
        return self.cur.fetchall()

    def db_cleanup(self):
        cleanup(self.db_file)
