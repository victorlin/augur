"""
Filter and subsample a sequence set.
"""
from Bio import SeqIO
from collections import defaultdict
import csv
import duckdb
import heapq
import itertools
import json
import numpy as np
import os
import pandas as pd
import random
import re
import sys
from tempfile import NamedTemporaryFile
import treetime.utils
from typing import Collection
from duckdb import DuckDBPyConnection

from .index import index_sequences, index_vcf
from .io import open_file, read_metadata, read_sequences, write_sequences
from .io_duckdb import load_tsv, DEFAULT_DB_FILE, METADATA_TABLE_NAME, SEQUENCE_INDEX_TABLE_NAME, FILTERED_VIEW_NAME, DATE_TABLE_NAME
from .utils import is_vcf as filename_is_vcf, read_vcf, read_strains, run_shell_command, shquote, is_date_ambiguous

comment_char = '#'

SEQUENCE_ONLY_FILTERS = (
    "min_length",
    "non_nucleotide",
)

DEFAULT_DATE_COL = "date"


class FilterException(Exception):
    """Representation of an error that occurred during filtering.
    """
    pass


def write_vcf(input_filename, output_filename, dropped_samps):
    if _filename_gz(input_filename):
        input_arg = "--gzvcf"
    else:
        input_arg = "--vcf"

    if _filename_gz(output_filename):
        output_pipe = "| gzip -c"
    else:
        output_pipe = ""

    drop_args = ["--remove-indv " + shquote(s) for s in dropped_samps]

    call = ["vcftools"] + drop_args + [input_arg, shquote(input_filename), "--recode --stdout", output_pipe, ">", shquote(output_filename)]

    print("Filtering samples using VCFTools with the call:")
    print(" ".join(call))
    run_shell_command(" ".join(call), raise_errors = True)
    # remove vcftools log file
    try:
        os.remove('out.log')
    except OSError:
        pass

def read_priority_scores(fname):
    try:
        with open(fname, encoding='utf-8') as pfile:
            return defaultdict(float, {
                elems[0]: float(elems[1])
                for elems in (line.strip().split('\t') if '\t' in line else line.strip().split() for line in pfile.readlines())
            })
    except Exception as e:
        print(f"ERROR: missing or malformed priority scores file {fname}", file=sys.stderr)
        raise e

# Define metadata filters.

def filter_by_exclude_all():
    """Exclude all strains regardless of the given metadata content.

    This is a placeholder function that can be called as part of a generalized
    loop through all possible functions.

    Returns
    -------
    str:
        expression for duckdb.filter
    """
    return 'False'


def exclude_strains_duckdb_filter(exclude_file):
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
    return f"strain NOT IN ({','.join(excluded_strains)})"


def parse_filter_query(query):
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


def exclude_where_duckdb_filter(exclude_where):
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
    column, op, value = parse_filter_query(exclude_where)
    op = '=' if op == '!=' else '!=' # negate for exclude
    return f"{column} {op} '{value}'"


def filter_by_ambiguous_date(metadata, date_column="date", ambiguity="any"):
    """Filter metadata in the given pandas DataFrame where values in the given date
    column have a given level of ambiguity.

    Parameters
    ----------
    metadata : pandas.DataFrame
        Metadata indexed by strain name
    date_column : str
        Column in the dataframe with dates.
    ambiguity : str
        Level of date ambiguity to filter metadata by

    Returns
    -------
    set[str]:
        Strains that pass the filter

    >>> metadata = pd.DataFrame([{"region": "Africa", "date": "2020-01-XX"}, {"region": "Europe", "date": "2020-01-02"}], index=["strain1", "strain2"])
    >>> filter_by_ambiguous_date(metadata)
    {'strain2'}
    >>> sorted(filter_by_ambiguous_date(metadata, ambiguity="month"))
    ['strain1', 'strain2']

    If the requested date column does not exist, we quietly skip this filter.

    >>> sorted(filter_by_ambiguous_date(metadata, date_column="missing_column"))
    ['strain1', 'strain2']

    """
    if date_column in metadata.columns:
        date_is_ambiguous = metadata[date_column].apply(
            lambda date: is_date_ambiguous(date, ambiguity)
        )
        filtered = set(metadata[~date_is_ambiguous].index.values)
    else:
        filtered = set(metadata.index.values)

    return filtered


def filter_by_min_date(min_date):
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
    return f"""strain IN (
        SELECT strain
        FROM {DATE_TABLE_NAME}
        WHERE date >= '{min_date}'
    )"""


def filter_by_max_date(max_date):
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
    return f"""strain IN (
        SELECT strain
        FROM {DATE_TABLE_NAME}
        WHERE date <= '{max_date}'
    )"""


def filter_by_sequence_index():
    """Filter metadata by presence of corresponding entries in a given sequence
    index. This filter effectively intersects the strain ids in the metadata and
    sequence index.

    Returns
    -------
    str:
        expression for duckdb.filter
    """
    # TODO: consider JOIN vs subquery if performance issues https://stackoverflow.com/q/3856164
    return f"strain IN (SELECT strain FROM {SEQUENCE_INDEX_TABLE_NAME})"


def filter_by_sequence_length(min_length=0):
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
    return f"""strain IN (
        SELECT strain
        FROM {SEQUENCE_INDEX_TABLE_NAME}
        WHERE A+C+G+T > {min_length}
    )"""


def filter_by_non_nucleotide():
    """Filter metadata for strains with invalid nucleotide content.

    Returns
    -------
    str:
        expression for duckdb.filter
    """
    return f"""strain IN (
        SELECT strain
        FROM {SEQUENCE_INDEX_TABLE_NAME}
        WHERE invalid_nucleotides = 0
    )"""


def include_strains_duckdb_filter(include_file):
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
    return f"strain IN ({','.join(included_strains)})"


def include_where_duckdb_filter(include_where):
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
    column, op, value = parse_filter_query(include_where)
    return f"{column} {op} '{value}'"


def construct_filters(args, use_sequences:bool, has_date_col:bool):
    """Construct lists of filters and inclusion criteria based on user-provided
    arguments.

    Parameters
    ----------
    args : argparse.Namespace
        Command line arguments provided by the user.
    use_sequences : boolean
        Sequences are used based on arguments.

    Returns
    -------
    list :
        A list of 2-element tuples with a callable to use as a filter and a
        dictionary of kwargs to pass to the callable.
    list :
        A list of 2-element tuples with a callable and dictionary of kwargs that
        determines whether to force include strains in the final output.

    """
    exclude_by = []
    include_by = []

    # Force include sequences specified in file(s).
    if args.include:
        # Collect the union of all given strains to include.
        for include_file in args.include:
            include_by.append(include_strains_duckdb_filter(include_file))

    # Add sequences with particular metadata attributes.
    if args.include_where:
        for include_where in args.include_where:
            include_by.append(include_where_duckdb_filter(include_where))

    # Exclude all strains by default.
    if args.exclude_all:
        exclude_by.append(filter_by_exclude_all())

    # Filter by sequence index.
    if use_sequences:
        exclude_by.append(filter_by_sequence_index())

    # Remove strains explicitly excluded by name.
    if args.exclude:
        for exclude_file in args.exclude:
            exclude_by.append(exclude_strains_duckdb_filter(exclude_file))

    # Exclude strain my metadata field like 'host=camel'.
    if args.exclude_where:
        for exclude_where in args.exclude_where:
            exclude_by.append(exclude_where_duckdb_filter(exclude_where))

    # Exclude strains by metadata, using SQL querying.
    if args.query:
        exclude_by.append(args.query)

    if has_date_col:
        # Filter by ambiguous dates.
        # TODO: SQL date filtering
        if args.exclude_ambiguous_dates_by:
            exclude_by.append((
                filter_by_ambiguous_date,
                {
                    "date_column": "date",
                    "ambiguity": args.exclude_ambiguous_dates_by,
                }
            ))

        # Filter by date.
        if args.min_date:
            exclude_by.append(filter_by_min_date(args.min_date))
        if args.max_date:
            exclude_by.append(filter_by_max_date(args.max_date))

    # Filter by sequence length.
    if args.min_length:
        # Skip VCF files and warn the user that the min length filter does not
        # make sense for VCFs.
        is_vcf = filename_is_vcf(args.sequences)

        if is_vcf: #doesn't make sense for VCF, ignore.
            print("WARNING: Cannot use min_length for VCF files. Ignoring...")
        else:
            exclude_by.append(filter_by_sequence_length(args.min_length))

    # Exclude sequences with non-nucleotide characters.
    if args.non_nucleotide:
        exclude_by.append(filter_by_non_nucleotide())

    return exclude_by, include_by


def check_date_col(connection:DuckDBPyConnection, date_column=DEFAULT_DATE_COL):
    metadata = connection.table(METADATA_TABLE_NAME)
    return date_column in metadata.columns


def generate_date_view(connection:DuckDBPyConnection, date_column=DEFAULT_DATE_COL):
    query = f"""
        SELECT
            d1.strain,
            d2.date,
            d1.year,
            d1.month,
            d1.day
        FROM (
            SELECT
                strain,
                string_split({date_column}, '-')[0] AS year,
                string_split({date_column}, '-')[1] AS month,
                string_split({date_column}, '-')[2] AS day
            FROM {METADATA_TABLE_NAME}
        ) d1
        LEFT OUTER JOIN (
            SELECT
                strain,
                {date_column}::date AS date
            FROM {METADATA_TABLE_NAME}
            WHERE regexp_matches({date_column}, '^\d{{4}}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$')
        ) d2
        ON (d1.strain = d2.strain);
    """
    # TODO: use VIEW when this is fixed https://github.com/duckdb/duckdb/issues/2860
    connection.execute(f"DROP TABLE IF EXISTS {DATE_TABLE_NAME}")
    connection.execute(f"CREATE TABLE {DATE_TABLE_NAME} AS {query}")


def apply_filters(connection:DuckDBPyConnection, exclude_by, include_by):
    """Apply a list of filters to exclude or force-include records from the given
    metadata and return the strains to keep, to exclude, and to force include.

    Parameters
    ----------
    connection : DuckDBPyConnection
        DuckDB connection with metadata table.
    exclude_by : list[str]
        A list of filter expressions for duckdb.filter.
    include_by : list[str]
        A list of filter expressions for duckdb.filter.
    Returns
    -------
    DuckDBPyRelation
        relation for filtered metadata
    """
    metadata = connection.table(METADATA_TABLE_NAME)

    # no exclusions
    if not exclude_by:
        return metadata

    rel_include = None
    for include in include_by:
        if not rel_include:
            rel_include = metadata.filter(include)
        else:
            rel_include = rel_include.union(metadata.filter(include))
    # rel_include.create_view("metadata_force_include")

    metadata = connection.table(METADATA_TABLE_NAME)
    rel_exclude = None
    for exclude in exclude_by:
        if not rel_exclude:
            rel_exclude = metadata.filter(exclude)
        else:
            rel_exclude = rel_exclude.filter(exclude)
    # rel_exclude.create_view("metadata_exclude_applied")

    # TODO: figure out parity for strains_to_force_include
    # TODO: figure out parity for strains_to_filter (reason for exclusion, used in final report output)

    if not include_by:
        return rel_exclude
    return rel_include.union(rel_exclude).create_view(FILTERED_VIEW_NAME)


def get_groups_for_subsampling(strains, metadata, group_by=None):
    """Return a list of groups for each given strain based on the corresponding
    metadata and group by column.

    Parameters
    ----------
    strains : list
        A list of strains to get groups for.
    metadata : pandas.DataFrame
        Metadata to inspect for the given strains.
    group_by : list
        A list of metadata (or calculated) columns to group records by.

    Returns
    -------
    dict :
        A mapping of strain names to tuples corresponding to the values of the strain's group.
    list :
        A list of dictionaries with strains that were skipped from grouping and the reason why (see also: `apply_filters` output).

    >>> strains = ["strain1", "strain2"]
    >>> metadata = pd.DataFrame([{"strain": "strain1", "date": "2020-01-01", "region": "Africa"}, {"strain": "strain2", "date": "2020-02-01", "region": "Europe"}]).set_index("strain")
    >>> group_by = ["region"]
    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata, group_by)
    >>> group_by_strain
    {'strain1': ('Africa',), 'strain2': ('Europe',)}
    >>> skipped_strains
    []

    If we group by year or month, these groups are calculated from the date
    string.

    >>> group_by = ["year", "month"]
    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata, group_by)
    >>> group_by_strain
    {'strain1': (2020, (2020, 1)), 'strain2': (2020, (2020, 2))}

    If we omit the grouping columns, the result will group by a dummy column.

    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata)
    >>> group_by_strain
    {'strain1': ('_dummy',), 'strain2': ('_dummy',)}

    If we try to group by columns that don't exist, we get an error.

    >>> group_by = ["missing_column"]
    >>> get_groups_for_subsampling(strains, metadata, group_by)
    Traceback (most recent call last):
      ...
    augur.filter.FilterException: The specified group-by categories (['missing_column']) were not found. No sequences-per-group sampling will be done.

    If we try to group by some columns that exist and some that don't, we allow
    grouping to continue and print a warning message to stderr.

    >>> group_by = ["year", "month", "missing_column"]
    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata, group_by)
    >>> group_by_strain
    {'strain1': (2020, (2020, 1), 'unknown'), 'strain2': (2020, (2020, 2), 'unknown')}

    If we group by year month and some records don't have that information in
    their date fields, we should skip those records from the group output and
    track which records were skipped for which reasons.

    >>> metadata = pd.DataFrame([{"strain": "strain1", "date": "", "region": "Africa"}, {"strain": "strain2", "date": "2020-02-01", "region": "Europe"}]).set_index("strain")
    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata, ["year"])
    >>> group_by_strain
    {'strain2': (2020,)}
    >>> skipped_strains
    [{'strain': 'strain1', 'filter': 'skip_group_by_with_ambiguous_year', 'kwargs': ''}]

    Similarly, if we group by month, we should skip records that don't have
    month information in their date fields.

    >>> metadata = pd.DataFrame([{"strain": "strain1", "date": "2020", "region": "Africa"}, {"strain": "strain2", "date": "2020-02-01", "region": "Europe"}]).set_index("strain")
    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata, ["month"])
    >>> group_by_strain
    {'strain2': ((2020, 2),)}
    >>> skipped_strains
    [{'strain': 'strain1', 'filter': 'skip_group_by_with_ambiguous_month', 'kwargs': ''}]

    """
    metadata = metadata.loc[strains]
    group_by_strain = {}
    skipped_strains = []

    if metadata.empty:
        return group_by_strain, skipped_strains

    if not group_by or group_by == ('_dummy',):
        group_by_strain = {strain: ('_dummy',) for strain in strains}
        return group_by_strain, skipped_strains

    group_by_set = set(group_by)

    # If we could not find any requested categories, we cannot complete subsampling.
    if 'date' not in metadata and group_by_set <= {'year', 'month'}:
        raise FilterException(f"The specified group-by categories ({group_by}) were not found. No sequences-per-group sampling will be done. Note that using 'year' or 'year month' requires a column called 'date'.")
    if not group_by_set & (set(metadata.columns) | {'year', 'month'}):
        raise FilterException(f"The specified group-by categories ({group_by}) were not found. No sequences-per-group sampling will be done.")

    # date requested
    if 'year' in group_by_set or 'month' in group_by_set:
        if 'date' not in metadata:
            # set year/month/day = unknown
            print(f"WARNING: A 'date' column could not be found to group-by year or month.", file=sys.stderr)
            print(f"Filtering by group may behave differently than expected!", file=sys.stderr)
            df_dates = pd.DataFrame({'year': 'unknown', 'month': 'unknown'}, index=metadata.index)
            metadata = pd.concat([metadata, df_dates], axis=1)
        else:
            # replace date with year/month/day as nullable ints
            date_cols = ['year', 'month', 'day']
            df_dates = metadata['date'].str.split('-', n=2, expand=True)
            df_dates = df_dates.set_axis(date_cols[:len(df_dates.columns)], axis=1)
            missing_date_cols = set(date_cols) - set(df_dates.columns)
            for col in missing_date_cols:
                df_dates[col] = pd.NA
            for col in date_cols:
                df_dates[col] = pd.to_numeric(df_dates[col], errors='coerce').astype(pd.Int64Dtype())
            metadata = pd.concat([metadata.drop('date', axis=1), df_dates], axis=1)
            if 'year' in group_by_set:
                # skip ambiguous years
                df_skip = metadata[metadata['year'].isnull()]
                metadata.dropna(subset=['year'], inplace=True)
                for strain in df_skip.index:
                    skipped_strains.append({
                        "strain": strain,
                        "filter": "skip_group_by_with_ambiguous_year",
                        "kwargs": "",
                    })
            if 'month' in group_by_set:
                # skip ambiguous months
                df_skip = metadata[metadata['month'].isnull()]
                metadata.dropna(subset=['month'], inplace=True)
                for strain in df_skip.index:
                    skipped_strains.append({
                        "strain": strain,
                        "filter": "skip_group_by_with_ambiguous_month",
                        "kwargs": "",
                    })
                # month = (year, month)
                metadata['month'] = list(zip(metadata['year'], metadata['month']))
            # TODO: support group by day

    unknown_groups = group_by_set - set(metadata.columns)
    if unknown_groups:
        print(f"WARNING: Some of the specified group-by categories couldn't be found: {', '.join(unknown_groups)}", file=sys.stderr)
        print("Filtering by group may behave differently than expected!", file=sys.stderr)
        for group in unknown_groups:
            metadata[group] = 'unknown'

    group_by_strain = dict(zip(metadata.index, metadata[group_by].apply(tuple, axis=1)))
    return group_by_strain, skipped_strains


class PriorityQueue:
    """A priority queue implementation that automatically replaces lower priority
    items in the heap with incoming higher priority items.

    Add a single record to a heap with a maximum of 2 records.

    >>> queue = PriorityQueue(max_size=2)
    >>> queue.add({"strain": "strain1"}, 0.5)
    1

    Add another record with a higher priority. The queue should be at its maximum
    size.

    >>> queue.add({"strain": "strain2"}, 1.0)
    2
    >>> queue.heap
    [(0.5, 0, {'strain': 'strain1'}), (1.0, 1, {'strain': 'strain2'})]
    >>> list(queue.get_items())
    [{'strain': 'strain1'}, {'strain': 'strain2'}]

    Add a higher priority record that causes the queue to exceed its maximum
    size. The resulting queue should contain the two highest priority records
    after the lowest priority record is removed.

    >>> queue.add({"strain": "strain3"}, 2.0)
    2
    >>> list(queue.get_items())
    [{'strain': 'strain2'}, {'strain': 'strain3'}]

    Add a record with the same priority as another record, forcing the duplicate
    to be resolved by removing the oldest entry.

    >>> queue.add({"strain": "strain4"}, 1.0)
    2
    >>> list(queue.get_items())
    [{'strain': 'strain4'}, {'strain': 'strain3'}]

    """
    def __init__(self, max_size):
        """Create a fixed size heap (priority queue)

        """
        self.max_size = max_size
        self.heap = []
        self.counter = itertools.count()

    def add(self, item, priority):
        """Add an item to the queue with a given priority.

        If adding the item causes the queue to exceed its maximum size, replace
        the lowest priority item with the given item. The queue stores items
        with an additional heap id value (a count) to resolve ties between items
        with equal priority (favoring the most recently added item).

        """
        heap_id = next(self.counter)

        if len(self.heap) >= self.max_size:
            heapq.heappushpop(self.heap, (priority, heap_id, item))
        else:
            heapq.heappush(self.heap, (priority, heap_id, item))

        return len(self.heap)

    def get_items(self):
        """Return each item in the queue in order.

        Yields
        ------
        Any
            Item stored in the queue.

        """
        for priority, heap_id, item in self.heap:
            yield item


def create_queues_by_group(groups, max_size, max_attempts=100, random_seed=None):
    """Create a dictionary of priority queues per group for the given maximum size.

    When the maximum size is fractional, probabilistically sample the maximum
    size from a Poisson distribution. Make at least the given number of maximum
    attempts to create queues for which the sum of their maximum sizes is
    greater than zero.

    Create queues for two groups with a fixed maximum size.

    >>> groups = ("2015", "2016")
    >>> queues = create_queues_by_group(groups, 2)
    >>> sum(queue.max_size for queue in queues.values())
    4

    Create queues for two groups with a fractional maximum size. Their total max
    size should still be an integer value greater than zero.

    >>> seed = 314159
    >>> queues = create_queues_by_group(groups, 0.1, random_seed=seed)
    >>> int(sum(queue.max_size for queue in queues.values())) > 0
    True

    A subsequent run of this function with the same groups and random seed
    should produce the same queues and queue sizes.

    >>> more_queues = create_queues_by_group(groups, 0.1, random_seed=seed)
    >>> [queue.max_size for queue in queues.values()] == [queue.max_size for queue in more_queues.values()]
    True

    """
    queues_by_group = {}
    total_max_size = 0
    attempts = 0

    if max_size < 1.0:
        random_generator = np.random.default_rng(random_seed)

    # For small fractional maximum sizes, it is possible to randomly select
    # maximum queue sizes that all equal zero. When this happens, filtering
    # fails unexpectedly. We make multiple attempts to create queues with
    # maximum sizes greater than zero for at least one queue.
    while total_max_size == 0 and attempts < max_attempts:
        for group in sorted(groups):
            if max_size < 1.0:
                queue_max_size = random_generator.poisson(max_size)
            else:
                queue_max_size = max_size

            queues_by_group[group] = PriorityQueue(queue_max_size)

        total_max_size = sum(queue.max_size for queue in queues_by_group.values())
        attempts += 1

    return queues_by_group


def register_arguments(parser):
    input_group = parser.add_argument_group("inputs", "metadata and sequences to be filtered")
    input_group.add_argument('--metadata', required=True, metavar="FILE", help="sequence metadata, as CSV or TSV")
    input_group.add_argument('--sequences', '-s', help="sequences in FASTA or VCF format")
    input_group.add_argument('--sequence-index', help="sequence composition report generated by augur index. If not provided, an index will be created on the fly.")
    input_group.add_argument('--metadata-chunk-size', type=int, default=100000, help="maximum number of metadata records to read into memory at a time. Increasing this number can speed up filtering at the cost of more memory used.")
    input_group.add_argument('--metadata-id-columns', default=["strain", "name"], nargs="+", help="names of valid metadata columns containing identifier information like 'strain' or 'name'")

    metadata_filter_group = parser.add_argument_group("metadata filters", "filters to apply to metadata")
    metadata_filter_group.add_argument(
        '--query',
        help="""Filter samples by attribute.
        Uses Pandas Dataframe querying, see https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#indexing-query for syntax.
        (e.g., --query "country == 'Colombia'" or --query "(country == 'USA' & (division == 'Washington'))")"""
    )
    metadata_filter_group.add_argument('--min-date', type=date_string, help="minimal cutoff for date, the cutoff date is inclusive; may be specified as an Augur-style numeric date (with the year as the integer part) or YYYY-MM-DD")
    metadata_filter_group.add_argument('--max-date', type=date_string, help="maximal cutoff for date, the cutoff date is inclusive; may be specified as an Augur-style numeric date (with the year as the integer part) or YYYY-MM-DD")
    metadata_filter_group.add_argument('--exclude-ambiguous-dates-by', choices=['any', 'day', 'month', 'year'],
                                help='Exclude ambiguous dates by day (e.g., 2020-09-XX), month (e.g., 2020-XX-XX), year (e.g., 200X-10-01), or any date fields. An ambiguous year makes the corresponding month and day ambiguous, too, even if those fields have unambiguous values (e.g., "201X-10-01"). Similarly, an ambiguous month makes the corresponding day ambiguous (e.g., "2010-XX-01").')
    metadata_filter_group.add_argument('--exclude', type=str, nargs="+", help="file(s) with list of strains to exclude")
    metadata_filter_group.add_argument('--exclude-where', nargs='+',
                                help="Exclude samples matching these conditions. Ex: \"host=rat\" or \"host!=rat\". Multiple values are processed as OR (matching any of those specified will be excluded), not AND")
    metadata_filter_group.add_argument('--exclude-all', action="store_true", help="exclude all strains by default. Use this with the include arguments to select a specific subset of strains.")
    metadata_filter_group.add_argument('--include', type=str, nargs="+", help="file(s) with list of strains to include regardless of priorities or subsampling")
    metadata_filter_group.add_argument('--include-where', nargs='+',
                                help="Include samples with these values. ex: host=rat. Multiple values are processed as OR (having any of those specified will be included), not AND. This rule is applied last and ensures any sequences matching these rules will be included.")

    sequence_filter_group = parser.add_argument_group("sequence filters", "filters to apply to sequence data")
    sequence_filter_group.add_argument('--min-length', type=int, help="minimal length of the sequences")
    sequence_filter_group.add_argument('--non-nucleotide', action='store_true', help="exclude sequences that contain illegal characters")

    subsample_group = parser.add_argument_group("subsampling", "options to subsample filtered data")
    subsample_group.add_argument('--group-by', nargs='+', help="categories with respect to subsample; two virtual fields, \"month\" and \"year\", are supported if they don't already exist as real fields but a \"date\" field does exist")
    subsample_limits_group = subsample_group.add_mutually_exclusive_group()
    subsample_limits_group.add_argument('--sequences-per-group', type=int, help="subsample to no more than this number of sequences per category")
    subsample_limits_group.add_argument('--subsample-max-sequences', type=int, help="subsample to no more than this number of sequences; can be used without the group_by argument")
    probabilistic_sampling_group = subsample_group.add_mutually_exclusive_group()
    probabilistic_sampling_group.add_argument('--probabilistic-sampling', action='store_true', help="Allow probabilistic sampling during subsampling. This is useful when there are more groups than requested sequences. This option only applies when `--subsample-max-sequences` is provided.")
    probabilistic_sampling_group.add_argument('--no-probabilistic-sampling', action='store_false', dest='probabilistic_sampling')
    subsample_group.add_argument('--priority', type=str, help="""tab-delimited file with list of priority scores for strains (e.g., "<strain>\\t<priority>") and no header.
    When scores are provided, Augur converts scores to floating point values, sorts strains within each subsampling group from highest to lowest priority, and selects the top N strains per group where N is the calculated or requested number of strains per group.
    Higher numbers indicate higher priority.
    Since priorities represent relative values between strains, these values can be arbitrary.""")
    subsample_group.add_argument('--subsample-seed', type=int, help="random number generator seed to allow reproducible subsampling (with same input data).")

    output_group = parser.add_argument_group("outputs", "possible representations of filtered data (at least one required)")
    output_group.add_argument('--output', '--output-sequences', '-o', help="filtered sequences in FASTA format")
    output_group.add_argument('--output-metadata', help="metadata for strains that passed filters")
    output_group.add_argument('--output-strains', help="list of strains that passed filters (no header)")
    output_group.add_argument('--output-log', help="tab-delimited file with one row for each filtered strain and the reason it was filtered. Keyword arguments used for a given filter are reported in JSON format in a `kwargs` column.")

    parser.set_defaults(probabilistic_sampling=True)


def validate_arguments(args):
    """Validate arguments and return a boolean representing whether all validation
    rules succeeded.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed arguments from argparse

    Returns
    -------
    bool :
        Validation succeeded.

    """
    # Don't allow sequence output when no sequence input is provided.
    if args.output and not args.sequences:
        print(
            "ERROR: You need to provide sequences to output sequences.",
            file=sys.stderr)
        return False

    # Confirm that at least one output was requested.
    if not any((args.output, args.output_metadata, args.output_strains)):
        print(
            "ERROR: You need to select at least one output.",
            file=sys.stderr)
        return False

    # Don't allow filtering on sequence-based information, if no sequences or
    # sequence index is provided.
    if not args.sequences and not args.sequence_index and any(getattr(args, arg) for arg in SEQUENCE_ONLY_FILTERS):
        print(
            "ERROR: You need to provide a sequence index or sequences to filter on sequence-specific information.",
            file=sys.stderr)
        return False

    # Set flags if VCF
    is_vcf = filename_is_vcf(args.sequences)

    ### Check users has vcftools. If they don't, a one-blank-line file is created which
    #   allows next step to run but error very badly.
    if is_vcf:
        from shutil import which
        if which("vcftools") is None:
            print("ERROR: 'vcftools' is not installed! This is required for VCF data. "
                  "Please see the augur install instructions to install it.",
                  file=sys.stderr)
            return False

    # If user requested grouping, confirm that other required inputs are provided, too.
    if args.group_by and not any((args.sequences_per_group, args.subsample_max_sequences)):
        print(
            "ERROR: You must specify a number of sequences per group or maximum sequences to subsample.",
            file=sys.stderr
        )
        return False

    return True


def run(args):
    '''
    filter and subsample a set of sequences into an analysis set
    '''
    # Validate arguments before attempting any I/O.
    if not validate_arguments(args):
        return 1

    # Determine whether the sequence index exists or whether should be
    # generated. We need to generate an index if the input sequences are in a
    # VCF, if sequence output has been requested (so we can filter strains by
    # sequences that are present), or if any other sequence-based filters have
    # been requested.
    use_sequences = args.sequence_index is not None
    sequence_strains = None
    sequence_index_path = args.sequence_index
    build_sequence_index = False
    is_vcf = filename_is_vcf(args.sequences)

    if sequence_index_path is None and args.sequences and not args.exclude_all:
        build_sequence_index = True
        use_sequences = True

    if build_sequence_index:
        # Generate the sequence index on the fly, for backwards compatibility
        # with older workflows that don't generate the index ahead of time.
        # Create a temporary index using a random filename to avoid collisions
        # between multiple filter commands.
        with NamedTemporaryFile(delete=False) as sequence_index_file:
            sequence_index_path = sequence_index_file.name

        print(
            "Note: You did not provide a sequence index, so Augur will generate one.",
            "You can generate your own index ahead of time with `augur index` and pass it with `augur filter --sequence-index`.",
            file=sys.stderr
        )

        if is_vcf:
            index_vcf(args.sequences, sequence_index_path)
        else:
            index_sequences(args.sequences, sequence_index_path)

    # Load the sequence index
    if use_sequences:
        load_tsv(sequence_index_path, SEQUENCE_INDEX_TABLE_NAME)

        # Remove temporary index file, if it exists.
        if build_sequence_index:
            os.unlink(sequence_index_path)

        # Calculate summary statistics needed for filtering.
        connection = duckdb.connect(DEFAULT_DB_FILE, read_only=True)
        sequence_index_table = connection.table(SEQUENCE_INDEX_TABLE_NAME)
        sequence_strains = set(sequence_index_table.project('strain').df().set_index('strain').index.values)
        connection.close()

    # Load metadata
    load_tsv(args.metadata, METADATA_TABLE_NAME)
    connection = duckdb.connect(DEFAULT_DB_FILE)
    has_date_col = check_date_col(connection)
    if has_date_col:
        generate_date_view(connection)

    # Setup filters.
    exclude_by, include_by = construct_filters(
        args,
        use_sequences,
        has_date_col
    )

    rel_metadata_filtered = apply_filters(connection, exclude_by, include_by)
    rel_metadata_filtered.execute()
    if args.output_strains:
        rel_metadata_filtered.project('strain').df().to_csv(args.output_strains, index=None, header=False)
    if args.output_metadata:
        rel_metadata_filtered.df().to_csv(args.output_metadata, sep='\t', index=None)
    return

    # TODO: args.group_by
    # TODO: args.sequences_per_group
    # TODO: priority queue
    # TODO: args.output_log
    # TODO: filter_counts

    metadata_reader = read_metadata(
        args.metadata,
        id_columns=args.metadata_id_columns,
        chunk_size=args.metadata_chunk_size,
    )
    for metadata in metadata_reader:
        # Maintain list of all strains seen.
        metadata_strains.update(set(metadata.index.values))

        # Filter metadata.
        seq_keep, sequences_to_filter, sequences_to_include = apply_filters(
            metadata,
            exclude_by,
            include_by,
        )
        valid_strains.update(seq_keep)

        # Track distinct strains to include, so we can write their
        # corresponding metadata, strains, or sequences later, as needed.
        distinct_sequences_to_include = {
            record["strain"]
            for record in sequences_to_include
        }
        all_sequences_to_include.update(distinct_sequences_to_include)

        # Track reasons for filtered or force-included strains, so we can
        # report total numbers filtered and included at the end. Optionally,
        # write out these reasons to a log file.
        for filtered_strain in itertools.chain(sequences_to_filter, sequences_to_include):
            filter_counts[(filtered_strain["filter"], filtered_strain["kwargs"])] += 1

            # Log the names of strains that were filtered or force-included,
            # so we can properly account for each strain (e.g., including
            # those that were initially filtered for one reason and then
            # included again for another reason).
            if args.output_log:
                output_log_writer.writerow(filtered_strain)

        if group_by:
            # If grouping, track the highest priority metadata records or
            # count the number of records per group. First, we need to get
            # the groups for the given records.
            try:
                group_by_strain, skipped_strains = get_groups_for_subsampling(
                    seq_keep,
                    metadata,
                    group_by,
                )

                # Track strains skipped during grouping, so users know why those
                # strains were excluded from the analysis.
                for skipped_strain in skipped_strains:
                    filter_counts[(skipped_strain["filter"], skipped_strain["kwargs"])] += 1
                    valid_strains.remove(skipped_strain["strain"])

                    if args.output_log:
                        output_log_writer.writerow(skipped_strain)

                if args.subsample_max_sequences and records_per_group is not None:
                    # Count the number of records per group. We will use this
                    # information to calculate the number of sequences per group
                    # for the given maximum number of requested sequences.
                    for group in group_by_strain.values():
                        records_per_group[group] += 1
                else:
                    # Track the highest priority records, when we already
                    # know the number of sequences allowed per group.
                    if queues_by_group is None:
                        queues_by_group = {}

                    for strain in sorted(group_by_strain.keys()):
                        # During this first pass, we do not know all possible
                        # groups will be, so we need to build each group's queue
                        # as we first encounter the group.
                        group = group_by_strain[strain]
                        if group not in queues_by_group:
                            queues_by_group[group] = PriorityQueue(
                                max_size=sequences_per_group,
                            )

                        queues_by_group[group].add(
                            metadata.loc[strain],
                            priorities[strain],
                        )
            except FilterException as error:
                # When we cannot group by the requested columns, we print a
                # warning to the user and continue without subsampling or
                # grouping. TODO: We should consider treating this case as
                # an actual error and exiting here with a nonzero code.
                group_by = False
                print(
                    f"WARNING: {error}",
                    file=sys.stderr,
                )

        # Always write out strains that are force-included. Additionally, if
        # we are not grouping, write out metadata and strains that passed
        # filters so far.
        strains_to_write = distinct_sequences_to_include
        if not group_by:
            strains_to_write = strains_to_write | seq_keep

        if args.output_metadata:
            # TODO: wrap logic to write metadata into its own function
            metadata.loc[strains_to_write].to_csv(
                args.output_metadata,
                sep="\t",
                header=metadata_header,
                mode=metadata_mode,
            )
            metadata_header = False
            metadata_mode = "a"

        if args.output_strains:
            # TODO: Output strains will no longer be ordered. This is a
            # small breaking change.
            for strain in strains_to_write:
                output_strains.write(f"{strain}\n")

    # In the worst case, we need to calculate sequences per group from the
    # requested maximum number of sequences and the number of sequences per
    # group. Then, we need to make a second pass through the metadata to find
    # the requested number of records.
    if args.subsample_max_sequences and records_per_group is not None:
        # Calculate sequences per group. If there are more groups than maximum
        # sequences requested, sequences per group will be a floating point
        # value and subsampling will be probabilistic.
        try:
            sequences_per_group, probabilistic_used = calculate_sequences_per_group(
                args.subsample_max_sequences,
                records_per_group.values(),
                args.probabilistic_sampling,
            )
        except TooManyGroupsError as error:
            print(f"ERROR: {error}", file=sys.stderr)
            sys.exit(1)

        if (probabilistic_used):
            print(f"Sampling probabilistically at {sequences_per_group:0.4f} sequences per group, meaning it is possible to have more than the requested maximum of {args.subsample_max_sequences} sequences after filtering.")
        else:
            print(f"Sampling at {sequences_per_group} per group.")

        if queues_by_group is None:
            # We know all of the possible groups now from the first pass through
            # the metadata, so we can create queues for all groups at once.
            queues_by_group = create_queues_by_group(
                records_per_group.keys(),
                sequences_per_group,
                random_seed=args.subsample_seed,
            )

        # Make a second pass through the metadata, only considering records that
        # have passed filters.
        metadata_reader = read_metadata(
            args.metadata,
            id_columns=args.metadata_id_columns,
            chunk_size=args.metadata_chunk_size,
        )
        for metadata in metadata_reader:
            # Recalculate groups for subsampling as we loop through the
            # metadata a second time. TODO: We could store these in memory
            # during the first pass, but we want to minimize overall memory
            # usage at the moment.
            seq_keep = set(metadata.index.values) & valid_strains
            group_by_strain, skipped_strains = get_groups_for_subsampling(
                seq_keep,
                metadata,
                group_by,
            )

            for strain in sorted(group_by_strain.keys()):
                group = group_by_strain[strain]
                queues_by_group[group].add(
                    metadata.loc[strain],
                    priorities[strain],
                )

    # If we have any records in queues, we have grouped results and need to
    # stream the highest priority records to the requested outputs.
    num_excluded_subsamp = 0
    if queues_by_group:
        # Populate the set of strains to keep from the records in queues.
        subsampled_strains = set()
        for group, queue in queues_by_group.items():
            records = []
            for record in queue.get_items():
                # Each record is a pandas.Series instance. Track the name of the
                # record, so we can output its sequences later.
                subsampled_strains.add(record.name)

                # Construct a data frame of records to simplify metadata output.
                records.append(record)

                if args.output_strains:
                    # TODO: Output strains will no longer be ordered. This is a
                    # small breaking change.
                    output_strains.write(f"{record.name}\n")

            # Write records to metadata output, if requested.
            if args.output_metadata and len(records) > 0:
                records = pd.DataFrame(records)
                records.to_csv(
                    args.output_metadata,
                    sep="\t",
                    header=metadata_header,
                    mode=metadata_mode,
                )
                metadata_header = False
                metadata_mode = "a"

        # Count and optionally log strains that were not included due to
        # subsampling.
        strains_filtered_by_subsampling = valid_strains - subsampled_strains
        num_excluded_subsamp = len(strains_filtered_by_subsampling)
        if output_log_writer:
            for strain in strains_filtered_by_subsampling:
                output_log_writer.writerow({
                    "strain": strain,
                    "filter": "subsampling",
                    "kwargs": "",
                })

        valid_strains = subsampled_strains

    # Force inclusion of specific strains after filtering and subsampling.
    valid_strains = valid_strains | all_sequences_to_include

    # Write output starting with sequences, if they've been requested. It is
    # possible for the input sequences and sequence index to be out of sync
    # (e.g., the index is a superset of the given sequences input), so we need
    # to update the set of strains to keep based on which strains are actually
    # available.
    if is_vcf:
        if args.output:
            # Get the samples to be deleted, not to keep, for VCF
            dropped_samps = list(sequence_strains - valid_strains)
            write_vcf(args.sequences, args.output, dropped_samps)
    elif args.sequences:
        sequences = read_sequences(args.sequences)

        # If the user requested sequence output, stream to disk all sequences
        # that passed all filters to avoid reading sequences into memory first.
        # Even if we aren't emitting sequences, we track the observed strain
        # names in the sequence file as part of the single pass to allow
        # comparison with the provided sequence index.
        if args.output:
            observed_sequence_strains = set()
            with open_file(args.output, "wt") as output_handle:
                for sequence in sequences:
                    observed_sequence_strains.add(sequence.id)

                    if sequence.id in valid_strains:
                        write_sequences(sequence, output_handle, 'fasta')
        else:
            observed_sequence_strains = {sequence.id for sequence in sequences}

        if sequence_strains != observed_sequence_strains:
            # Warn the user if the expected strains from the sequence index are
            # not a superset of the observed strains.
            if sequence_strains is not None and observed_sequence_strains > sequence_strains:
                print(
                    "WARNING: The sequence index is out of sync with the provided sequences.",
                    "Metadata and strain output may not match sequence output.",
                    file=sys.stderr
                )

            # Update the set of available sequence strains.
            sequence_strains = observed_sequence_strains

    # Calculate the number of strains that don't exist in either metadata or
    # sequences.
    num_excluded_by_lack_of_metadata = 0
    if sequence_strains:
        # Update strains to keep based on available sequence data. This prevents
        # writing out strain lists or metadata for strains that have no
        # sequences.
        valid_strains = valid_strains & sequence_strains

        num_excluded_by_lack_of_metadata = len(sequence_strains - metadata_strains)

    if args.output_strains:
        output_strains.close()

    # Calculate the number of strains passed and filtered.
    total_strains_passed = len(valid_strains)
    total_strains_filtered = len(metadata_strains) + num_excluded_by_lack_of_metadata - total_strains_passed

    print(f"{total_strains_filtered} strains were dropped during filtering")

    if num_excluded_by_lack_of_metadata:
        print(f"\t{num_excluded_by_lack_of_metadata} had no metadata")

    report_template_by_filter_name = {
        "filter_by_sequence_index": "{count} had no sequence data",
        "filter_by_exclude_all": "{count} of these were dropped by `--exclude-all`",
        "filter_by_exclude": "{count} of these were dropped because they were in {exclude_file}",
        "filter_by_exclude_where": "{count} of these were dropped because of '{exclude_where}'",
        "filter_by_query": "{count} of these were filtered out by the query: \"{query}\"",
        "filter_by_ambiguous_date": "{count} of these were dropped because of their ambiguous date in {ambiguity}",
        "filter_by_date": "{count} of these were dropped because of their date (or lack of date)",
        "filter_by_sequence_length": "{count} of these were dropped because they were shorter than minimum length of {min_length}bp",
        "filter_by_non_nucleotide": "{count} of these were dropped because they had non-nucleotide characters",
        "skip_group_by_with_ambiguous_year": "{count} were dropped during grouping due to ambiguous year information",
        "skip_group_by_with_ambiguous_month": "{count} were dropped during grouping due to ambiguous month information",
        "include": "{count} strains were added back because they were in {include_file}",
        "include_by_include_where": "{count} sequences were added back because of '{include_where}'",
    }
    for (filter_name, filter_kwargs), count in filter_counts.items():
        if filter_kwargs:
            parameters = dict(json.loads(filter_kwargs))
        else:
            parameters = {}

        parameters["count"] = count
        print("\t" + report_template_by_filter_name[filter_name].format(**parameters))

    if (group_by and args.sequences_per_group) or args.subsample_max_sequences:
        seed_txt = ", using seed {}".format(args.subsample_seed) if args.subsample_seed else ""
        print("\t%i of these were dropped because of subsampling criteria%s" % (num_excluded_subsamp, seed_txt))

    if total_strains_passed == 0:
        print("ERROR: All samples have been dropped! Check filter rules and metadata file format.", file=sys.stderr)
        return 1

    print(f"{total_strains_passed} strains passed all filters")


def _filename_gz(filename):
    return filename.lower().endswith(".gz")


def date_string(date):
    """
    Converts the given *date* to a :py:class:`string` in the YYYY-MM-DD (ISO 8601) syntax.
    """
    if type(date) is float or type(date) is int:
        return treetime.utils.datestring_from_numeric(date)
    if type(date) is str:
        return date # TODO: verify


def calculate_sequences_per_group(target_max_value, counts_per_group, allow_probabilistic=True):
    """Calculate the number of sequences per group for a given maximum number of
    sequences to be returned and the number of sequences in each requested
    group. Optionally, allow the result to be probabilistic such that the mean
    result of a Poisson process achieves the calculated sequences per group for
    the given maximum.

    Parameters
    ----------
    target_max_value : int
        Maximum number of sequences to return by subsampling at some calculated
        number of sequences per group for the given counts per group.
    counts_per_group : list[int]
        A list with the number of sequences in each requested group.
    allow_probabilistic : bool
        Whether to allow probabilistic subsampling when the number of groups
        exceeds the requested maximum.

    Raises
    ------
    TooManyGroupsError :
        When there are more groups than sequences per group and probabilistic
        subsampling is not allowed.

    Returns
    -------
    int or float :
        Number of sequences per group.
    bool :
        Whether probabilistic subsampling was used.

    """
    probabilistic_used = False

    try:
        sequences_per_group = _calculate_sequences_per_group(
            target_max_value,
            counts_per_group,
        )
    except TooManyGroupsError as error:
        if allow_probabilistic:
            print(f"WARNING: {error}")
            sequences_per_group = _calculate_fractional_sequences_per_group(
                target_max_value,
                counts_per_group,
            )
            probabilistic_used = True
        else:
            raise error

    return sequences_per_group, probabilistic_used


class TooManyGroupsError(ValueError):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return str(self.msg)


def _calculate_total_sequences(
        hypothetical_spg: float, sequence_lengths: Collection[int],
) -> float:
    # calculate how many sequences we'd keep given a hypothetical spg.
    return sum(
        min(hypothetical_spg, sequence_length)
        for sequence_length in sequence_lengths
    )


def _calculate_sequences_per_group(
        target_max_value: int,
        sequence_lengths: Collection[int]
) -> int:
    """This is partially inspired by
    https://github.com/python/cpython/blob/3.8/Lib/bisect.py

    This should return the spg such that we don't exceed the requested
    number of samples.

    Parameters
    ----------
    target_max_value : int
        the total number of sequences allowed across all groups
    sequence_lengths : Collection[int]
        the number of sequences in each group

    Returns
    -------
    int
        maximum number of sequences allowed per group to meet the required maximum total
        sequences allowed

    >>> _calculate_sequences_per_group(4, [4, 2])
    2
    >>> _calculate_sequences_per_group(2, [4, 2])
    1
    >>> _calculate_sequences_per_group(1, [4, 2])
    Traceback (most recent call last):
        ...
    augur.filter.TooManyGroupsError: Asked to provide at most 1 sequences, but there are 2 groups.
    """

    if len(sequence_lengths) > target_max_value:
        # we have more groups than sequences we are allowed, which is an
        # error.

        raise TooManyGroupsError(
            "Asked to provide at most {} sequences, but there are {} "
            "groups.".format(target_max_value, len(sequence_lengths)))

    lo = 1
    hi = target_max_value

    while hi - lo > 2:
        mid = (hi + lo) // 2
        if _calculate_total_sequences(mid, sequence_lengths) <= target_max_value:
            lo = mid
        else:
            hi = mid

    if _calculate_total_sequences(hi, sequence_lengths) <= target_max_value:
        return int(hi)
    else:
        return int(lo)


def _calculate_fractional_sequences_per_group(
        target_max_value: int,
        sequence_lengths: Collection[int]
) -> float:
    """Returns the fractional sequences per group for the given list of group
    sequences such that the total doesn't exceed the requested number of
    samples.

    Parameters
    ----------
    target_max_value : int
        the total number of sequences allowed across all groups
    sequence_lengths : Collection[int]
        the number of sequences in each group

    Returns
    -------
    float
        fractional maximum number of sequences allowed per group to meet the
        required maximum total sequences allowed

    >>> np.around(_calculate_fractional_sequences_per_group(4, [4, 2]), 4)
    1.9375
    >>> np.around(_calculate_fractional_sequences_per_group(2, [4, 2]), 4)
    0.9688

    Unlike the integer-based version of this function, the fractional version
    can accept a maximum number of sequences that exceeds the number of groups.
    In this case, the function returns a fraction that can be used downstream,
    for example with Poisson sampling.

    >>> np.around(_calculate_fractional_sequences_per_group(1, [4, 2]), 4)
    0.4844
    """
    lo = 1e-5
    hi = target_max_value

    while (hi / lo) > 1.1:
        mid = (lo + hi) / 2
        if _calculate_total_sequences(mid, sequence_lengths) <= target_max_value:
            lo = mid
        else:
            hi = mid

    return (lo + hi) / 2
