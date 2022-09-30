import numpy as np
import pandas as pd
from typing import List, Set

from augur.io.print import print_err
from ...errors import FilterException


def get_sizes_per_group(df_groups:pd.DataFrame, size_col:str, max_size, max_attempts=100, random_seed=None):
    """Create a DataFrame of sizes per group for the given maximum size.

    When the maximum size is fractional, probabilistically sample the maximum
    size from a Poisson distribution. Make at least the given number of maximum
    attempts to create groups for which the sum of their maximum sizes is
    greater than zero.

    Parameters
    ----------
    df_groups : pd.DataFrame
        DataFrame with one row per group.
    size_col : str
        Name of new column for group size.
    max_size : int | float
        Maximum size of a group.
    max_attempts : int
        Maximum number of attempts for creating group sizes.
    random_seed
        Seed value for np.random.default_rng for reproducible randomness.

    Returns
    -------
    pd.DataFrame
        df_groups with an additional column (size_col) containing group size.
    """
    total_max_size = 0
    attempts = 0

    # For small fractional maximum sizes, it is possible to randomly select
    # maximum sizes that all equal zero. When this happens, filtering
    # fails unexpectedly. We make multiple attempts to create sizes with
    # maximum sizes greater than zero for at least one group.
    while total_max_size == 0 and attempts < max_attempts:
        if max_size < 1.0:
            df_groups[size_col] = np.random.default_rng(random_seed).poisson(max_size, size=len(df_groups))
        else:
            df_groups[size_col] = max_size
        total_max_size = sum(df_groups[size_col])
        attempts += 1

    # TODO: raise error if total_max_size is still 0
    return df_groups


def get_valid_group_by_cols(group_by_cols:List[str], metadata_cols: Set[str]):
    """Perform validation on requested group-by columns and return the valid subset.

    Parameters
    ----------
    group_by_cols
        Column names requested for grouping.
    metadata_cols
        All column names in metadata.

    Returns
    -------
    list(str):
        Valid group-by columns.
    """
    # TODO: change behavior to address https://github.com/nextstrain/augur/issues/754
    extracted_date_cols = {'year', 'month'}
    group_by_set = set(group_by_cols)
    if group_by_set <= extracted_date_cols and 'date' not in metadata_cols:
        # all requested group-by columns are extracted date columns, but the date column is missing
        raise FilterException(f"The specified group-by categories ({group_by_cols}) were not found. Note that using 'year' or 'year month' requires a column called 'date'.")
    if not group_by_set & (metadata_cols | extracted_date_cols):
        # none of the requested group-by columns are valid
        raise FilterException(f"The specified group-by categories ({group_by_cols}) were not found.")
    unknown_cols = list(group_by_set - metadata_cols - extracted_date_cols)
    if 'date' not in metadata_cols:
        if "year" in group_by_set:
            print_err("WARNING: A 'date' column could not be found to group-by year.")
            unknown_cols.append("year")
        if "month" in group_by_set:
            print_err("WARNING: A 'date' column could not be found to group-by month.")
            unknown_cols.append("month")
    if unknown_cols:
        # warn and skip unknown columns
        print_err(f"WARNING: Some of the specified group-by categories couldn't be found: {', '.join(unknown_cols)}")
        print_err("Filtering by group may behave differently than expected!")
        valid_group_by_cols = list(group_by_cols)  # copy to preserve input object
        for col in unknown_cols:
            valid_group_by_cols.remove(col)
        return valid_group_by_cols
    return group_by_cols
