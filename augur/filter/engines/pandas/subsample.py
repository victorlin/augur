import heapq
import itertools
import sys
import numpy as np
import pandas as pd

from ...errors import FilterException


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
    augur.filter.errors.FilterException: The specified group-by categories (['missing_column']) were not found.

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
    metadata = metadata.loc[list(strains)]
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
        raise FilterException(f"The specified group-by categories ({group_by}) were not found. Note that using 'year' or 'year month' requires a column called 'date'.")
    if not group_by_set & (set(metadata.columns) | {'year', 'month'}):
        raise FilterException(f"The specified group-by categories ({group_by}) were not found.")

    # date requested
    if 'year' in group_by_set or 'month' in group_by_set:

        if 'year' in metadata.columns and 'year' in group_by_set:
            print(f"WARNING: `--group-by year` uses the generated year value from the 'date' column. The custom 'year' column in the metadata is ignored for grouping purposes.", file=sys.stderr)
            metadata.drop('year', axis=1, inplace=True)
        if 'month' in metadata.columns and 'month' in group_by_set:
            print(f"WARNING: `--group-by month` uses the generated month value from the 'date' column. The custom 'month' column in the metadata is ignored for grouping purposes.", file=sys.stderr)
            metadata.drop('month', axis=1, inplace=True)

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
