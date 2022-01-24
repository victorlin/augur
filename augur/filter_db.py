import abc
import os
from typing import List
import numpy as np
import pandas as pd
from tempfile import NamedTemporaryFile
import argparse
from .index import index_sequences, index_vcf
from .io import print_err
from .utils import is_vcf
from .filter_subsample_helpers import calculate_sequences_per_group


METADATA_TABLE_NAME = 'metadata'
SEQUENCE_INDEX_TABLE_NAME = 'sequence_index'
PRIORITIES_TABLE_NAME = 'priorities'
DATE_TABLE_NAME = 'metadata_date_expanded'
GROUP_SIZES_TABLE_NAME = 'group_sizes'
OUTPUT_METADATA_TABLE_NAME = 'metadata_output'

EXTENDED_VIEW_NAME = 'metadata_filtered_extended'

DEFAULT_DATE_COL = 'date'
DUMMY_COL = 'dummy'
GROUP_SIZE_COL = 'size'
STRAIN_COL = 'strain'
PRIORITY_COL = 'priority'


class FilterDB(abc.ABC):
    def __init__(self, args:argparse.Namespace):
        self.args = args

    def run(self):
        self.db_connect()
        self.db_load_metadata()
        self.add_attributes()
        self.handle_sequences()
        if self.has_date_col:
            self.db_create_date_table()
        exclude_by, include_by = self.construct_filters()
        self.db_create_filter_reason_table(exclude_by, include_by)
        if self.do_subsample:
            self.subsample()
        self.db_create_output_table()
        # TODO: args.output_log
        # TODO: args.output (sequences)
        # TODO: filter_counts
        self.write_outputs()
        self.write_report()
        self.db_cleanup()

    @abc.abstractmethod
    def db_connect(self): pass

    @abc.abstractmethod
    def db_load_metadata(self): pass

    @abc.abstractmethod
    def db_load_sequence_index(self, path): pass

    def add_attributes(self):
        """Check if there is a date column and if sequences are used."""
        self.has_date_col = self.db_has_date_col()
        self.use_sequences = bool(self.args.sequence_index or (self.args.sequences and not self.args.exclude_all))
        self.do_subsample = bool(self.args.group_by or self.args.subsample_max_sequences)

    @abc.abstractmethod
    def db_has_date_col(self): pass

    def handle_sequences(self):
        """Load sequence index"""
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
            self.db_load_sequence_index(sequence_index_path)

            # Remove temporary index file, if it exists.
            if build_sequence_index:
                os.unlink(sequence_index_path)

    @abc.abstractmethod
    def db_create_date_table(self): pass

    def construct_filters(self):
        exclude_by = []
        include_by = []

        # Force include sequences specified in file(s).
        if self.args.include:
            # Collect the union of all given strains to include.
            for include_file in self.args.include:
                include_by.append((self.force_include_strains.__name__, self.force_include_strains(include_file)))

        # Add sequences with particular metadata attributes.
        if self.args.include_where:
            for include_where in self.args.include_where:
                include_by.append((self.force_include_where.__name__, self.force_include_where(include_where)))

        # Exclude all strains by default.
        if self.args.exclude_all:
            exclude_by.append((self.filter_by_exclude_all.__name__, self.filter_by_exclude_all()))

        # Filter by sequence index.
        if self.use_sequences:
            exclude_by.append((self.filter_by_sequence_index.__name__, self.filter_by_sequence_index()))

        # Remove strains explicitly excluded by name.
        if self.args.exclude:
            for exclude_file in self.args.exclude:
                exclude_by.append((self.filter_by_exclude_strains.__name__, self.filter_by_exclude_strains(exclude_file)))

        # Exclude strain my metadata field like 'host=camel'.
        if self.args.exclude_where:
            for exclude_where in self.args.exclude_where:
                exclude_by.append((self.filter_by_exclude_where.__name__, self.filter_by_exclude_where(exclude_where)))

        # Exclude strains by metadata, using SQL querying.
        if self.args.query:
            exclude_by.append((self.filter_by_query.__name__, self.filter_by_query(self.args.query)))

        # TODO: check if no date column but filters require it
        if self.has_date_col:
            # Filter by ambiguous dates.
            if self.args.exclude_ambiguous_dates_by:
                exclude_by.append((self.filter_by_ambiguous_date.__name__, self.filter_by_ambiguous_date(self.args.exclude_ambiguous_dates_by)))

            # Filter by date.
            if self.args.min_date:
                exclude_by.append((self.filter_by_min_date.__name__, self.filter_by_min_date(self.args.min_date)))
            if self.args.max_date:
                exclude_by.append((self.filter_by_max_date.__name__, self.filter_by_max_date(self.args.max_date)))

        # Filter by sequence length.
        if self.args.min_length:
            if is_vcf(self.args.sequences):
                print("WARNING: Cannot use min_length for VCF files. Ignoring...")
            else:
                exclude_by.append((self.filter_by_sequence_length.__name__, self.filter_by_sequence_length(self.args.min_length)))

        if self.args.group_by:
            if "month" in self.args.group_by:
                exclude_by.append((self.skip_group_by_with_ambiguous_month.__name__, self.skip_group_by_with_ambiguous_month()))
            if "year" in self.args.group_by:
                exclude_by.append((self.skip_group_by_with_ambiguous_year.__name__, self.skip_group_by_with_ambiguous_year()))

        # Exclude sequences with non-nucleotide characters.
        if self.args.non_nucleotide:
            exclude_by.append((self.filter_by_non_nucleotide.__name__, self.filter_by_non_nucleotide()))

        return exclude_by, include_by

    @abc.abstractmethod
    def filter_by_exclude_all(self): pass

    @abc.abstractmethod
    def filter_by_exclude_strains(self, exclude_file): pass

    @abc.abstractmethod
    def parse_filter_query(self, query): pass

    @abc.abstractmethod
    def filter_by_exclude_where(self, exclude_where): pass

    @abc.abstractmethod
    def filter_by_query(self, query): pass

    @abc.abstractmethod
    def filter_by_ambiguous_date(self, ambiguity="any"): pass

    @abc.abstractmethod
    def filter_by_min_date(self, min_date): pass

    @abc.abstractmethod
    def filter_by_max_date(self, max_date): pass

    @abc.abstractmethod
    def filter_by_sequence_index(self): pass

    @abc.abstractmethod
    def filter_by_sequence_length(self, min_length=0): pass

    @abc.abstractmethod
    def filter_by_non_nucleotide(self): pass

    @abc.abstractmethod
    def force_include_strains(self, include_file): pass

    @abc.abstractmethod
    def force_include_where(self, include_where): pass

    def skip_group_by_with_ambiguous_month(self):
        """Alias to filter_by_ambiguous_date(ambiguity="month") with a specific function name for filter reason."""
        return self.filter_by_ambiguous_date(ambiguity="month")

    def skip_group_by_with_ambiguous_year(self):
        """Alias to filter_by_ambiguous_date(ambiguity="year") with a specific function name for filter reason."""
        return self.filter_by_ambiguous_date(ambiguity="year")

    @abc.abstractmethod
    def db_create_filter_reason_table(self, exclude_by:List[str], include_by:List[str]): pass

    @abc.abstractmethod
    def db_create_output_table(self, input_table:str): pass

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
                # might not be needed
                counts_per_group = [self.db_get_filtered_strains_count()]

            sequences_per_group, probabilistic_used = calculate_sequences_per_group(
                self.args.subsample_max_sequences,
                counts_per_group,
                allow_probabilistic=self.args.probabilistic_sampling
            )

        self.db_create_group_sizes_table(group_by_cols, sequences_per_group)
        self.db_update_filter_reason_table_with_subsampling(group_by_cols)

    @abc.abstractmethod
    def db_get_counts_per_group(self, group_by_cols:List[str]): pass

    @abc.abstractmethod
    def db_get_filtered_strains_count(self): pass

    @abc.abstractmethod
    def db_update_filter_reason_table_with_subsampling(self, group_by_cols:List[str]): pass

    def create_priorities_table(self):
        if self.args.priority:
            self.db_load_priorities_table()
        else:
            self.db_generate_priorities_table(self.args.subsample_seed)

    @abc.abstractmethod
    def db_load_priorities_table(self): pass

    @abc.abstractmethod
    def db_generate_priorities_table(self, seed:int=None): pass

    @abc.abstractmethod
    def db_create_extended_filtered_metadata_view(self): pass

    @abc.abstractmethod
    def db_create_group_sizes_table(self, group_by:list, sequences_per_group:float): pass

    def write_outputs(self):
        if self.args.output_strains:
            self.db_output_strains()
        if self.args.output_metadata:
            self.db_output_metadata()

    @abc.abstractmethod
    def db_get_total_strains_passed(self): pass

    @abc.abstractmethod
    def db_get_num_excluded_by_lack_of_metadata(self): pass

    @abc.abstractmethod
    def db_get_num_metadata_strains(self): pass

    @abc.abstractmethod
    def db_get_num_excluded_subsamp(self): pass

    def write_report(self):
        total_strains_passed = self.db_get_total_strains_passed()
        num_excluded_by_lack_of_metadata = self.db_get_num_excluded_by_lack_of_metadata()
        num_metadata_strains = self.db_get_num_metadata_strains()
        num_excluded_subsamp = self.db_get_num_excluded_subsamp()
        filter_counts = dict() # TODO: filter_kwargs_to_str

        # Calculate the number of strains passed and filtered.
        total_strains_filtered = num_metadata_strains + num_excluded_by_lack_of_metadata - total_strains_passed

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
                import json
                parameters = dict(json.loads(filter_kwargs))
            else:
                parameters = {}

            parameters["count"] = count
            print("\t" + report_template_by_filter_name[filter_name].format(**parameters))

        if self.do_subsample:
            seed_txt = ", using seed {}".format(self.args.subsample_seed) if self.args.subsample_seed else ""
            print("\t%i of these were dropped because of subsampling criteria%s" % (num_excluded_subsamp, seed_txt))

        if total_strains_passed == 0:
            print_err("ERROR: All samples have been dropped! Check filter rules and metadata file format.")
            return 1

        print(f"{total_strains_passed} strains passed all filters")

    @abc.abstractmethod
    def db_output_strains(self): pass

    @abc.abstractmethod
    def db_output_metadata(self): pass

    @abc.abstractmethod
    def db_cleanup(self): pass

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
