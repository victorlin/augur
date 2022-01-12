"""
Filter and subsample a sequence set.
"""
import os
import sys
import treetime.utils
from augur.filter_duckdb import FilterDuckDB

from .utils import is_vcf, run_shell_command, shquote


SEQUENCE_ONLY_FILTERS = (
    "min_length",
    "non_nucleotide",
)


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

    ### Check users has vcftools. If they don't, a one-blank-line file is created which
    #   allows next step to run but error very badly.
    if is_vcf(args.sequences):
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

    filter = FilterDuckDB(args)
    filter.run()


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
