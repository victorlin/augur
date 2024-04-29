"""
Filter and subsample a sequence set.
"""
from augur.argparse_ import ExtendOverwriteDefault
from augur.dates import numeric_date_type, SUPPORTED_DATE_HELP_TEXT
from augur.filter.io import ACCEPTED_TYPES, column_type_pair
from augur.io.metadata import DEFAULT_DELIMITERS, DEFAULT_ID_COLUMNS, METADATA_DATE_COLUMN
from augur.types import EmptyOutputReportingMethod
from . import constants


def register_arguments(parser):
    """
    Add arguments to parser.
    Kept as a separate function than `register_parser` to continue to support
    unit tests that use this function to create argparser.
    """
    input_group = parser.add_argument_group("inputs", "metadata and sequences to be filtered")
    input_group.add_argument('--metadata', required=True, metavar="FILE", help="sequence metadata")
    input_group.add_argument('--sequences', '-s', help="sequences in FASTA or VCF format")
    input_group.add_argument('--sequence-index', help="sequence composition report generated by augur index. If not provided, an index will be created on the fly.")
    input_group.add_argument('--metadata-chunk-size', type=int, default=100000, help="maximum number of metadata records to read into memory at a time. Increasing this number can speed up filtering at the cost of more memory used.")
    input_group.add_argument('--metadata-id-columns', default=DEFAULT_ID_COLUMNS, nargs="+", action=ExtendOverwriteDefault, help="names of possible metadata columns containing identifier information, ordered by priority. Only one ID column will be inferred.")
    input_group.add_argument('--metadata-delimiters', default=DEFAULT_DELIMITERS, nargs="+", action=ExtendOverwriteDefault, help="delimiters to accept when reading a metadata file. Only one delimiter will be inferred.")

    metadata_filter_group = parser.add_argument_group("metadata filters", "filters to apply to metadata")
    metadata_filter_group.add_argument(
        '--query',
        help="""Filter samples by attribute.
        Uses Pandas Dataframe querying, see https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#indexing-query for syntax.
        (e.g., --query "country == 'Colombia'" or --query "(country == 'USA' & (division == 'Washington'))")"""
    )
    metadata_filter_group.add_argument('--query-columns', type=column_type_pair, nargs="+", action="extend", help=f"""
        Use alongside --query to specify columns and data types in the format 'column:type', where type is one of ({','.join(ACCEPTED_TYPES)}).
        Automatic type inference will be attempted on all unspecified columns used in the query.
        Example: region:str coverage:float.
    """)
    metadata_filter_group.add_argument('--min-date', type=numeric_date_type, help=f"minimal cutoff for date, the cutoff date is inclusive; may be specified as: {SUPPORTED_DATE_HELP_TEXT}")
    metadata_filter_group.add_argument('--max-date', type=numeric_date_type, help=f"maximal cutoff for date, the cutoff date is inclusive; may be specified as: {SUPPORTED_DATE_HELP_TEXT}")
    metadata_filter_group.add_argument('--exclude-ambiguous-dates-by', choices=['any', 'day', 'month', 'year'],
                                help='Exclude ambiguous dates by day (e.g., 2020-09-XX), month (e.g., 2020-XX-XX), year (e.g., 200X-10-01), or any date fields. An ambiguous year makes the corresponding month and day ambiguous, too, even if those fields have unambiguous values (e.g., "201X-10-01"). Similarly, an ambiguous month makes the corresponding day ambiguous (e.g., "2010-XX-01").')
    metadata_filter_group.add_argument('--exclude', type=str, nargs="+", action="extend", help="file(s) with list of strains to exclude")
    metadata_filter_group.add_argument('--exclude-where', nargs='+', action='extend',
                                help="Exclude samples matching these conditions. Ex: \"host=rat\" or \"host!=rat\". Multiple values are processed as OR (matching any of those specified will be excluded), not AND")
    metadata_filter_group.add_argument('--exclude-all', action="store_true", help="exclude all strains by default. Use this with the include arguments to select a specific subset of strains.")
    metadata_filter_group.add_argument('--include', type=str, nargs="+", action="extend", help="file(s) with list of strains to include regardless of priorities, subsampling, or absence of an entry in --sequences.")
    metadata_filter_group.add_argument('--include-where', nargs='+', action='extend', help="""
        Include samples with these values. ex: host=rat. Multiple values are
        processed as OR (having any of those specified will be included), not
        AND. This rule is applied last and ensures any strains matching these
        rules will be included regardless of priorities, subsampling, or absence
        of an entry in --sequences.""")

    sequence_filter_group = parser.add_argument_group("sequence filters", "filters to apply to sequence data")
    sequence_filter_group.add_argument('--min-length', type=int, help="minimal length of the sequences, only counting standard nucleotide characters A, C, G, or T (case-insensitive)")
    sequence_filter_group.add_argument('--max-length', type=int, help="maximum length of the sequences, only counting standard nucleotide characters A, C, G, or T (case-insensitive)")
    sequence_filter_group.add_argument('--non-nucleotide', action='store_true', help="exclude sequences that contain illegal characters")

    subsample_group = parser.add_argument_group("subsampling", "options to subsample filtered data")
    subsample_group.add_argument('--group-by', nargs='+', action='extend', default=[], help=f"""
        categories with respect to subsample.
        Notes:
        (1) Grouping by {sorted(constants.GROUP_BY_GENERATED_COLUMNS)} is only supported when there is a {METADATA_DATE_COLUMN!r} column in the metadata.
        (2) 'week' uses the ISO week numbering system, where a week starts on a Monday and ends on a Sunday.
        (3) 'month' and 'week' grouping cannot be used together.
        (4) Custom columns {sorted(constants.GROUP_BY_GENERATED_COLUMNS)} in the metadata are ignored for grouping. Please rename them if you want to use their values for grouping.""")
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

    output_group = parser.add_argument_group("outputs", "options related to outputs, at least one of the possible representations of filtered data (--output, --output-metadata, --output-strains) is required")
    output_group.add_argument('--output', '--output-sequences', '-o', help="filtered sequences in FASTA format")
    output_group.add_argument('--output-metadata', help="metadata for strains that passed filters")
    output_group.add_argument('--output-strains', help="list of strains that passed filters (no header)")
    output_group.add_argument('--output-log', help="tab-delimited file with one row for each filtered strain and the reason it was filtered. Keyword arguments used for a given filter are reported in JSON format in a `kwargs` column.")
    output_group.add_argument(
        '--empty-output-reporting',
        type=EmptyOutputReportingMethod.argtype,
        choices=list(EmptyOutputReportingMethod),
        default=EmptyOutputReportingMethod.ERROR,
        help="How should empty outputs be reported when no strains pass filtering and/or subsampling.")

    parser.set_defaults(probabilistic_sampling=True)


def register_parser(parent_subparsers):
    parser = parent_subparsers.add_parser("filter", help=__doc__)
    register_arguments(parser)
    return parser


def run(args):
    '''
    filter and subsample a set of sequences into an analysis set
    '''
    from .validate_arguments import validate_arguments
    # Validate arguments before attempting any I/O.
    validate_arguments(args)

    from ._run import run as _run
    return _run(args)
