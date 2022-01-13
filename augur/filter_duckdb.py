import os
import re
from typing import List
import duckdb
import numpy as np
import pandas as pd
from tempfile import NamedTemporaryFile
import argparse
from .index import index_sequences, index_vcf
from .io import print_err
from .io_duckdb import load_tsv, DEFAULT_DB_FILE
from .utils import is_vcf, read_strains
from .filter_subsample_helpers import calculate_sequences_per_group, get_sizes_per_group


METADATA_TABLE_NAME = 'metadata'
SEQUENCE_INDEX_TABLE_NAME = 'sequence_index'
PRIORITIES_TABLE_NAME = 'priorities'
DATE_TABLE_NAME = 'metadata_date_expanded'
GROUP_SIZES_TABLE_NAME = 'group_sizes'
FILTERED_TABLE_NAME = 'metadata_filtered'
SUBSAMPLED_TABLE_NAME = 'metadata_subsampled'
OUTPUT_METADATA_TABLE_NAME = 'metadata_output'

EXTENDED_VIEW_NAME = 'metadata_filtered_extended'
SUBSAMPLE_STRAINS_VIEW_NAME = 'subsample_strains'

DEFAULT_DATE_COL = 'date'
DUMMY_COL = 'dummy'
GROUP_SIZE_COL = 'size'
STRAIN_COL = 'strain'
PRIORITY_COL = 'priority'


class FilterDuckDB():
    def __init__(self, args:argparse.Namespace):
        self.args = args
        self.connection = duckdb.connect(DEFAULT_DB_FILE)

    def run(self):
        self.load_metadata()
        self.add_attributes()
        self.handle_sequences()
        if self.has_date_col:
            self.db_create_date_table()
        exclude_by, include_by = self.construct_filters() # keep abstract filtering logic in construct_filters
            # or have a class method per filter type?
        self.db_create_filtered_view(exclude_by, include_by)
        if self.args.group_by or self.args.subsample_max_sequences:
            self.subsample()
            self.db_create_output_table(SUBSAMPLED_TABLE_NAME)
        else:
            self.db_create_output_table(FILTERED_TABLE_NAME)
        # TODO: args.output_log
        # TODO: args.output (sequences)
        # TODO: filter_counts
        self.write_outputs()

    def db_load_table(self, path:str, name:str):
        load_tsv(self.connection, path, name)

    def load_metadata(self):
        self.db_load_table(self.args.metadata, METADATA_TABLE_NAME)

    def add_attributes(self):
        self.has_date_col = self.db_has_date_col()
        self.use_sequences = bool(self.args.sequence_index or (self.args.sequences and not self.args.exclude_all))

    def db_has_date_col(self):
        return (DEFAULT_DATE_COL in self.connection.table(METADATA_TABLE_NAME).columns)

    def handle_sequences(self):
        sequence_index_path = self.args.sequence_index
        build_sequence_index = False

        if sequence_index_path is None and self.use_sequences:
            build_sequence_index = True

        if build_sequence_index:
            # Generate the sequence index on the fly, for backwards compatibility
            # with older workflows that don't generate the index ahead of time.
            # Create a temporary index using a random filename to avoid collisions
            # between multiple filter commands.
            with NamedTemporaryFile(delete=False) as sequence_index_file:
                sequence_index_path = sequence_index_file.name

            print_err(
                "Note: You did not provide a sequence index, so Augur will generate one.",
                "You can generate your own index ahead of time with `augur index` and pass it with `augur filter --sequence-index`."
            )

            if is_vcf(self.args.sequences):
                index_vcf(self.args.sequences, sequence_index_path)
            else:
                index_sequences(self.args.sequences, sequence_index_path)

        if self.use_sequences:
            # TODO: verify VCF
            self.db_load_table(sequence_index_path, SEQUENCE_INDEX_TABLE_NAME)

            # Remove temporary index file, if it exists.
            if build_sequence_index:
                os.unlink(sequence_index_path)

    def db_create_date_table(self):
        metadata = self.connection.table(METADATA_TABLE_NAME)
        # create temporary table to generate date columns
        tmp_table = "tmp"
        rel_tmp = metadata.project(f"""
            {STRAIN_COL},
            {DEFAULT_DATE_COL},
            0::BIGINT as year,
            0::BIGINT as month,
            0::BIGINT as day,
            '' as date_min,
            '' as date_max
        """)
        rel_tmp = rel_tmp.map(populate_date_cols)
        rel_tmp.execute()
        rel_tmp.create(tmp_table)
        # unable to cast to date type before creating table, possibly related to https://github.com/duckdb/duckdb/issues/2860
        # so we create the actual date table here
        rel = self.connection.table(tmp_table)
        rel = rel.project(f"""
            {STRAIN_COL},
            year,
            month,
            day,
            date_min::DATE as date_min,
            date_max::DATE as date_max
        """)
        rel.execute()
        self.connection.execute(f"DROP TABLE IF EXISTS {DATE_TABLE_NAME}")
        rel.create(DATE_TABLE_NAME)
        self.connection.execute(f"DROP TABLE IF EXISTS {tmp_table}")

    def construct_filters(self):
        exclude_by = []
        include_by = []

        # Force include sequences specified in file(s).
        if self.args.include:
            # Collect the union of all given strains to include.
            for include_file in self.args.include:
                include_by.append(self.include_strains_duckdb_filter(include_file))

        # Add sequences with particular metadata attributes.
        if self.args.include_where:
            for include_where in self.args.include_where:
                include_by.append(self.include_where_duckdb_filter(include_where))

        # Exclude all strains by default.
        if self.args.exclude_all:
            exclude_by.append(self.filter_by_exclude_all())

        # Filter by sequence index.
        if self.use_sequences:
            exclude_by.append(self.exclude_by_sequence_index())

        # Remove strains explicitly excluded by name.
        if self.args.exclude:
            for exclude_file in self.args.exclude:
                exclude_by.append(self.exclude_strains_duckdb_filter(exclude_file))

        # Exclude strain my metadata field like 'host=camel'.
        if self.args.exclude_where:
            for exclude_where in self.args.exclude_where:
                exclude_by.append(self.exclude_where_duckdb_filter(exclude_where))

        # Exclude strains by metadata, using SQL querying.
        if self.args.query:
            exclude_by.append(self.args.query)

        if self.has_date_col:
            # Filter by ambiguous dates.
            if self.args.exclude_ambiguous_dates_by:
                exclude_by.append(self.exclude_by_ambiguous_date(self.args.exclude_ambiguous_dates_by))

            # Filter by date.
            if self.args.min_date:
                exclude_by.append(self.exclude_by_min_date(self.args.min_date))
            if self.args.max_date:
                exclude_by.append(self.exclude_by_max_date(self.args.max_date))

        # Filter by sequence length.
        if self.args.min_length:
            if is_vcf(self.args.sequences):
                print("WARNING: Cannot use min_length for VCF files. Ignoring...")
            else:
                exclude_by.append(self.exclude_by_sequence_length(self.args.min_length))

        # Exclude sequences with non-nucleotide characters.
        if self.args.non_nucleotide:
            exclude_by.append(self.exclude_by_non_nucleotide())

        return exclude_by, include_by

    def filter_by_exclude_all(self):
        """Exclude all strains regardless of the given metadata content.

        This is a placeholder function that can be called as part of a generalized
        loop through all possible functions.

        Returns
        -------
        str:
            expression for duckdb.filter
        """
        return 'False'

    def exclude_strains_duckdb_filter(self, exclude_file):
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
        return f"{STRAIN_COL} NOT IN ({','.join(excluded_strains)})"

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

    def exclude_where_duckdb_filter(self, exclude_where):
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
        op = '=' if op == '!=' else '!=' # negate for exclude
        return f"{column} {op} '{value}'"

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
                WHERE year IS NOT NULL
            )"""
        if ambiguity == 'month':
            return f"""{STRAIN_COL} IN (
                SELECT {STRAIN_COL}
                FROM {DATE_TABLE_NAME}
                WHERE month IS NOT NULL AND year IS NOT NULL
            )"""
        if ambiguity == 'day' or ambiguity == 'any':
            return f"""{STRAIN_COL} IN (
                SELECT {STRAIN_COL}
                FROM {DATE_TABLE_NAME}
                WHERE day IS NOT NULL AND month IS NOT NULL AND year IS NOT NULL
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
            WHERE date_min >= '{min_date}'
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
            WHERE date_max <= '{max_date}'
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
        return f"{STRAIN_COL} IN (SELECT {STRAIN_COL} FROM {SEQUENCE_INDEX_TABLE_NAME})"

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
            WHERE A+C+G+T > {min_length}
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
            WHERE invalid_nucleotides = 0
        )"""

    def include_strains_duckdb_filter(self, include_file):
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

    def include_where_duckdb_filter(self, include_where):
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
        return f"{column} {op} '{value}'"

    def db_create_filtered_view(self, exclude_by:List[str], include_by:List[str]):
        rel_filtered = self.db_apply_filters(exclude_by, include_by)
        rel_filtered.execute()
        self.connection.execute(f"DROP TABLE IF EXISTS {FILTERED_TABLE_NAME}")
        rel_filtered.create(FILTERED_TABLE_NAME)

    def db_apply_filters(self, exclude_by:List[str], include_by:List[str]):
        """Apply a list of filters to exclude or force-include records from the given
        metadata and return the strains to keep, to exclude, and to force include.

        Parameters
        ----------
        exclude_by : list[str]
            A list of filter expressions for duckdb.filter.
        include_by : list[str]
            A list of filter expressions for duckdb.filter.
        Returns
        -------
        DuckDBPyRelation
            relation for filtered metadata
        """
        metadata = self.connection.table(METADATA_TABLE_NAME)

        # no exclusions
        if not exclude_by:
            return metadata

        rel_exclude = metadata # start with all rows
        for exclude in exclude_by:
            rel_exclude = rel_exclude.filter(exclude)

        # no force-inclusions
        if not include_by:
            return rel_exclude

        rel_include = metadata.limit(0) # start with 0 rows
        for include in include_by:
            rel_include = rel_include.union(metadata.filter(include))

        # TODO: figure out parity for strains_to_force_include
        # TODO: figure out parity for strains_to_filter (reason for exclusion, used in final report output)

        # exclusions + force-inclusions
        return rel_exclude.union(rel_include)

    def db_create_output_table(self, input_table:str):
        rel_input = self.connection.table(input_table)
        self.connection.execute(f"DROP TABLE IF EXISTS {OUTPUT_METADATA_TABLE_NAME}")
        rel_input.create(OUTPUT_METADATA_TABLE_NAME)

    def subsample(self):
        self.create_priorities_table()
        self.db_create_extended_filtered_metadata_view()

        group_by_cols = self.args.group_by
        sequences_per_group = self.args.sequences_per_group

        if self.args.subsample_max_sequences:
            if self.args.group_by:
                counts_per_group = self.db_get_counts_per_group(group_by_cols)
            else:
                group_by_cols = [DUMMY_COL]
                counts_per_group = [self.db_get_filtered_strains_count()]

            sequences_per_group, probabilistic_used = calculate_sequences_per_group(
                self.args.subsample_max_sequences,
                counts_per_group,
                allow_probabilistic=self.args.probabilistic_sampling
            )

        self.db_create_group_sizes_table(group_by_cols, sequences_per_group)
        self.db_create_subsampled_table(group_by_cols)

    def db_get_counts_per_group(self, group_by_cols:List[str]):
        count_col = 'count'
        df = self.connection.view(EXTENDED_VIEW_NAME).aggregate(f"{','.join(group_by_cols)}, COUNT(*) AS {count_col}").df()
        return df[count_col].values

    def db_get_filtered_strains_count(self):
        return self.connection.table(FILTERED_TABLE_NAME).aggregate('COUNT(*)').df().iloc[0,0]

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
        self.connection.execute(f"CREATE OR REPLACE VIEW {SUBSAMPLE_STRAINS_VIEW_NAME} AS {query}")
        # use subsample strains to select rows from filtered metadata
        rel_output = self.connection.table(FILTERED_TABLE_NAME).filter(f'{STRAIN_COL} IN (SELECT {STRAIN_COL} FROM {SUBSAMPLE_STRAINS_VIEW_NAME})')
        rel_output.execute()
        self.connection.execute(f"DROP TABLE IF EXISTS {SUBSAMPLED_TABLE_NAME}")
        rel_output.create(SUBSAMPLED_TABLE_NAME)

    def create_priorities_table(self):
        if self.args.priority:
            self.db_load_priorities_table()
        else:
            self.db_generate_priorities_table(self.args.subsample_seed)

    def db_load_priorities_table(self):
        load_tsv(self.connection, self.args.priority, PRIORITIES_TABLE_NAME, header=False, names=[STRAIN_COL, PRIORITY_COL])

    def db_generate_priorities_table(self, seed:int=None):
        if seed:
            self.connection.execute(f"SELECT setseed({seed})")
        self.connection.execute(f"DROP TABLE IF EXISTS {PRIORITIES_TABLE_NAME}")
        self.connection.execute(f"""
            CREATE TABLE {PRIORITIES_TABLE_NAME} AS
            SELECT {STRAIN_COL}, RANDOM() AS {PRIORITY_COL}
            FROM {FILTERED_TABLE_NAME}
        """)

    def db_create_extended_filtered_metadata_view(self):
        # create new view that extends strain with year/month/day, priority, dummy
        self.connection.execute(f"""
        CREATE OR REPLACE VIEW {EXTENDED_VIEW_NAME} AS (
            SELECT m.*, d.year, d.month, d.day, p.{PRIORITY_COL}, TRUE AS {DUMMY_COL}
            FROM {FILTERED_TABLE_NAME} m
            JOIN {DATE_TABLE_NAME} d ON m.{STRAIN_COL} = d.{STRAIN_COL}
            LEFT OUTER JOIN {PRIORITIES_TABLE_NAME} p ON m.{STRAIN_COL} = p.{STRAIN_COL}
        )
        """)

    def db_create_group_sizes_table(self, group_by:list, sequences_per_group:float):
        df_groups = self.connection.view(EXTENDED_VIEW_NAME).aggregate(','.join(group_by)).df()
        df_sizes = get_sizes_per_group(df_groups, GROUP_SIZE_COL, sequences_per_group, random_seed=self.args.subsample_seed)
        self.connection.execute(f"DROP TABLE IF EXISTS {GROUP_SIZES_TABLE_NAME}")
        self.connection.from_df(df_sizes).create(GROUP_SIZES_TABLE_NAME)
        # TODO: check if connection.register as a view is sufficient

    def write_outputs(self):
        if self.args.output_strains:
            self.db_output_strains()
        if self.args.output_metadata:
            self.db_output_metadata()

    def db_output_strains(self):
        rel_output = self.connection.table(OUTPUT_METADATA_TABLE_NAME)
        rel_output.project(STRAIN_COL).df().to_csv(self.args.output_strains, index=None, header=False)

    def db_output_metadata(self):
        rel_output = self.connection.table(OUTPUT_METADATA_TABLE_NAME)
        rel_output.df().to_csv(self.args.output_metadata, sep='\t', index=None)

def populate_date_cols(df:pd.DataFrame):
    if df.empty:
        return df  # sometimes duckdb makes empty passes
    df_date_parts = get_date_parts(df)
    for col in df_date_parts.columns:
        df[col] = df_date_parts[col]
    return df


def get_date_parts(df:pd.DataFrame) -> pd.DataFrame:
    """Expand the date column of a DataFrame to minimum and maximum date (ISO 8601 format) based on potential ambiguity.
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing date column.
    Returns
    -------
    pandas.DataFrame :
        The input metadata with expanded date columns.
    """
    # TODO: np.where to convert numerical dates
    # TODO: BC dates
    # replace date with year/month/day as nullable ints
    date_cols = ['year', 'month', 'day']
    df_date_parts = df[DEFAULT_DATE_COL].str.split('-', n=2, expand=True)
    df_date_parts = df_date_parts.set_axis(date_cols[:len(df_date_parts.columns)], axis=1)
    missing_date_cols = set(date_cols) - set(df_date_parts.columns)
    for col in missing_date_cols:
        df_date_parts[col] = pd.NA
    for col in date_cols:
        df_date_parts[col] = pd.to_numeric(df_date_parts[col], errors='coerce').astype(pd.Int64Dtype())
    year_str = df_date_parts['year'].astype(str)
    min_month = df_date_parts['month'].fillna(1).astype(str).str.zfill(2)
    min_day = df_date_parts['day'].fillna(1).astype(str).str.zfill(2)
    # set max month=12 if missing
    max_month = df_date_parts['month'].fillna(12)
    # set max day based on max month
    max_day_na_fill = np.where(
        max_month.isin([1,3,5,7,8,10,12]), 31,
        np.where(
            max_month.eq(2), 28,
            30
        )
    )
    max_day = df_date_parts['day'].fillna(pd.Series(max_day_na_fill)).astype(str).str.zfill(2)
    max_month = max_month.astype(str).str.zfill(2)
    df_date_parts['date_min'] = np.where(
        df_date_parts['year'].notna(),
        year_str.str.cat([min_month, min_day], sep="-"),
        None)
    df_date_parts['date_max'] = np.where(
        df_date_parts['year'].notna(),
        year_str.str.cat([max_month, max_day], sep="-"),
        None)
    return df_date_parts
