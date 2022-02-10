"""
Filter and subsample a sequence set.
"""
from augur.filter_support.date_parsing import date_type
from augur.filter_support.db.sqlite import FilterSQLite


def register_arguments(parser):
    input_group = parser.add_argument_group("inputs", "metadata and sequences to be filtered")
    input_group.add_argument('--metadata', required=True, metavar="FILE", help="sequence metadata, as CSV or TSV")
    input_group.add_argument('--sequences', '-s', help="sequences in FASTA or VCF format")
    input_group.add_argument('--sequence-index', help="sequence composition report generated by augur index. If not provided, an index will be created on the fly.")
    input_group.add_argument('--metadata-id-columns', default=["strain", "name"], nargs="+", help="names of valid metadata columns containing identifier information like 'strain' or 'name'")

    metadata_filter_group = parser.add_argument_group("metadata filters", "filters to apply to metadata")
    metadata_filter_group.add_argument(
        '--query',
        help="""Filter samples by attribute.
        Uses SQL WHERE clause querying, see https://www.sqlite.org/lang_expr.html for syntax.
        (e.g., --query "country = 'Colombia'" or --query "(country = 'USA' AND division = 'Washington')")"""
    )
    metadata_filter_group.add_argument('--min-date', type=date_type, help="minimal cutoff for date, the cutoff date is inclusive; may be specified as an Augur-style numeric date (with the year as the integer part) or YYYY-MM-DD")
    metadata_filter_group.add_argument('--max-date', type=date_type, help="maximal cutoff for date, the cutoff date is inclusive; may be specified as an Augur-style numeric date (with the year as the integer part) or YYYY-MM-DD")
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


def run(args):
    '''
    filter and subsample a set of sequences into an analysis set
    '''
    filter = FilterSQLite()
    filter.set_args(args)
    filter.try_run()
