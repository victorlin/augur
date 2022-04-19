from textwrap import dedent
from freezegun import freeze_time
import pytest
import augur.filter
from augur.dates import (
    any_to_numeric_type_min,
    any_to_numeric_type_max,
)
from augur.filter_support.db.sqlite import (
    EXCLUDE_COL,
    FILTER_REASON_COL,
    FILTER_REASON_KWARGS_COL,
    INCLUDE_COL,
    METADATA_FILTER_REASON_TABLE_NAME,
)
from augur.filter_support.exceptions import FilterException

from test_filter import (
    parse_args,
    get_filter_obj_run,
    get_valid_args,
    query_fetchall,
    write_file,
    write_metadata,
)


class TestFiltering:
    def test_filter_by_query(self, tmpdir):
        """Filter by a query expresssion."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        args.query = 'quality=="good"'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_query'
        """)
        assert results == [("SEQ_2",)]

    def test_filter_by_query_two_conditions(self, tmpdir):
        """Filter by a query expresssion with two conditions."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        args.query = 'quality=="good" AND location=="colorado"'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_query'
        """)
        assert results == [("SEQ_2",), ("SEQ_3",)]

    def test_filter_by_query_and_include_strains(self, tmpdir):
        """Filter by a query expresssion and force-include a strain."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        include_fn = str(tmpdir / "include.txt")
        open(include_fn, "w").write("SEQ_3")
        args = get_valid_args(data, tmpdir)
        args.query = 'quality=="good" AND location=="colorado"'
        args.include = [include_fn]
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        assert results == [("SEQ_1",), ("SEQ_3",)]

    def test_filter_by_query_and_include_where(self, tmpdir):
        """Filter by a query expresssion and force-include a strain."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        args.query = 'quality=="good" AND location=="colorado"'
        args.include_where = ['location=nevada']
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        assert results == [("SEQ_1",), ("SEQ_3",)]

    def test_filter_by_min_date(self, tmpdir):
        """Filter by min date, inclusive."""
        data = [("strain","date"),
                ("SEQ_1","2020-02-XX"),
                ("SEQ_2","2020-02-26"),
                ("SEQ_3","2020-02-25"),
                ("SEQ_4","?")]
        args = get_valid_args(data, tmpdir)
        args.min_date = any_to_numeric_type_min('2020-02-26')
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_min_date'
        """)
        assert results == [("SEQ_3",), ("SEQ_4",)]

    def test_filter_by_max_date(self, tmpdir):
        """Filter by max date, inclusive."""
        data = [("strain","date"),
                ("SEQ_1","2020-03-XX"),
                ("SEQ_2","2020-03-01"),
                ("SEQ_3","2020-03-02"),
                ("SEQ_4","?")]
        args = get_valid_args(data, tmpdir)
        args.max_date = any_to_numeric_type_max('2020-03-01')
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_max_date'
        """)
        assert results == [("SEQ_3",), ("SEQ_4",)]

    def test_filter_incomplete_year(self, tmpdir):
        """Test that 2020 is evaluated as 2020-XX-XX"""
        data = [("strain","date"),
                ("SEQ_1","2020.0"),
                ("SEQ_2","2020"),
                ("SEQ_3","2020-XX-XX")]
        args = get_valid_args(data, tmpdir)
        args.min_date = any_to_numeric_type_min('2020-02-01')
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_min_date'
        """)
        assert results == [("SEQ_1",)]

    def test_filter_date_formats(self, tmpdir):
        """Test that 2020.0, 2020, and 2020-XX-XX all pass --min-date 2019"""
        data = [("strain","date"),
                ("SEQ_1","2020.0"),
                ("SEQ_2","2020"),
                ("SEQ_3","2020-XX-XX")]
        args = get_valid_args(data, tmpdir)
        args.min_date = any_to_numeric_type_min('2019')
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_min_date'
        """)
        assert results == []

    @freeze_time("2020-03-25")
    @pytest.mark.parametrize(
        "argparse_params, metadata_rows, output_sorted_expected",
        [
            (
                "--min-date 1D",
                (
                    ("SEQ_1","2020-03-23"),
                    ("SEQ_2","2020-03-24"),
                    ("SEQ_3","2020-03-25"),
                ),
                ["SEQ_2", "SEQ_3"],
            ),
            (
                "--max-date 1D",
                (
                    ("SEQ_1","2020-03-23"),
                    ("SEQ_2","2020-03-24"),
                    ("SEQ_3","2020-03-25"),
                ),
                ["SEQ_1", "SEQ_2"],
            ),
            (
                "--min-date 4W",
                (
                    ("SEQ_1","2020-02-25"),
                    ("SEQ_2","2020-02-26"),
                    ("SEQ_3","2020-03-25"),
                ),
                ["SEQ_2", "SEQ_3"],
            ),
            (
                "--max-date 4W",
                (
                    ("SEQ_1","2020-02-25"),
                    ("SEQ_2","2020-02-26"),
                    ("SEQ_3","2020-03-25"),
                ),
                ["SEQ_1", "SEQ_2"],
            ),
            (
                "--min-date 1M",
                (
                    ("SEQ_1","2020-01-25"),
                    ("SEQ_2","2020-02-25"),
                    ("SEQ_3","2020-03-25"),
                ),
                ["SEQ_2", "SEQ_3"],
            ),
            (
                "--max-date 1M",
                (
                    ("SEQ_1","2020-01-25"),
                    ("SEQ_2","2020-02-25"),
                    ("SEQ_3","2020-03-25"),
                ),
                ["SEQ_1", "SEQ_2"],
            ),
            (
                "--min-date P1M",
                (
                    ("SEQ_1","2020-01-25"),
                    ("SEQ_2","2020-02-25"),
                    ("SEQ_3","2020-03-25"),
                ),
                ["SEQ_2", "SEQ_3"],
            ),
            (
                "--max-date P1M",
                (
                    ("SEQ_1","2020-01-25"),
                    ("SEQ_2","2020-02-25"),
                    ("SEQ_3","2020-03-25"),
                ),
                ["SEQ_1", "SEQ_2"],
            ),
            (
                "--min-date 2Y",
                (
                    ("SEQ_1","2017-03-25"),
                    ("SEQ_2","2018-03-25"),
                    ("SEQ_3","2019-03-25"),
                ),
                ["SEQ_2", "SEQ_3"],
            ),
            (
                "--max-date 2Y",
                (
                    ("SEQ_1","2017-03-25"),
                    ("SEQ_2","2018-03-25"),
                    ("SEQ_3","2019-03-25"),
                ),
                ["SEQ_1", "SEQ_2"],
            ),
            (
                "--min-date 1Y2W5D",
                (
                    ("SEQ_1","2019-03-05"),
                    ("SEQ_2","2019-03-06"),
                    ("SEQ_3","2019-03-07"),
                ),
                ["SEQ_2", "SEQ_3"],
            ),
            (
                "--max-date 1Y2W5D",
                (
                    ("SEQ_1","2019-03-05"),
                    ("SEQ_2","2019-03-06"),
                    ("SEQ_3","2019-03-07"),
                ),
                ["SEQ_1", "SEQ_2"],
            ),
        ],
    )
    def test_filter_relative_dates(self, tmpdir, argparse_params, metadata_rows, output_sorted_expected):
        """Test that various relative dates work"""
        out_fn = str(tmpdir / "filtered.txt")
        meta_fn = write_metadata(tmpdir, (("strain","date"),
                                          *metadata_rows))
        args = parse_args(f'--metadata {meta_fn} --output-strains {out_fn} {argparse_params}')
        augur.filter.run(args)
        with open(out_fn) as f:
            output_sorted = sorted(line.rstrip() for line in f)
        assert output_sorted == output_sorted_expected

    @freeze_time("2020-03-25")
    @pytest.mark.parametrize(
        "argparse_flag, argparse_value",
        [
            ("--min-date", "3000Y"),
            ("--max-date", "3000Y"),
            ("--min-date", "invalid"),
            ("--max-date", "invalid"),
        ],
    )
    def test_filter_relative_dates_error(self, tmpdir, argparse_flag, argparse_value):
        """Test that invalid dates fail"""
        out_fn = str(tmpdir / "filtered.txt")
        meta_fn = write_metadata(tmpdir, (("strain","date"),
                                          ("SEQ_1","2020-03-23")))
        with pytest.raises(SystemExit) as e_info:
            parse_args(f'--metadata {meta_fn} --output-strains {out_fn} {argparse_flag} {argparse_value}')
        assert e_info.value.__context__.message == dedent(f"""\
            Unable to determine date from '{argparse_value}'. Ensure it is in one of the supported formats:
            1. an Augur-style numeric date with the year as the integer part (e.g. 2020.42) or
            2. a date in ISO 8601 date format (i.e. YYYY-MM-DD) (e.g. '2020-06-04') or
            3. a backwards-looking relative date in ISO 8601 duration format with optional P prefix (e.g. '1W', 'P1W')
        """)

    def test_filter_by_ambiguous_date_year(self, tmpdir):
        """Filter out dates with ambiguous year."""
        data = [("strain","date"),
                ("SEQ_1","XXXX"),
                ("SEQ_2","2020-XX"),
                ("SEQ_3","2020-03-XX"),
                ("SEQ_4","?")]
        args = get_valid_args(data, tmpdir)
        args.exclude_ambiguous_dates_by = 'year'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain, {FILTER_REASON_KWARGS_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_ambiguous_date'
        """)
        assert results == [
            ('SEQ_1', '[["ambiguity", "year"]]'),
            ('SEQ_4', '[["ambiguity", "year"]]')
        ]

    def test_filter_by_ambiguous_date_month(self, tmpdir):
        """Filter out dates with ambiguous month."""
        data = [("strain","date"),
                ("SEQ_1","XXXX"),
                ("SEQ_2","2020-XX"),
                ("SEQ_3","2020-03-XX"),
                ("SEQ_4","?")]
        args = get_valid_args(data, tmpdir)
        args.exclude_ambiguous_dates_by = 'month'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain, {FILTER_REASON_KWARGS_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_ambiguous_date'
        """)
        assert results == [
            ('SEQ_1', '[["ambiguity", "month"]]'),
            ('SEQ_2', '[["ambiguity", "month"]]'),
            ('SEQ_4', '[["ambiguity", "month"]]')
        ]

    def test_filter_by_ambiguous_date_day(self, tmpdir):
        """Filter out dates with ambiguous day."""
        data = [("strain","date"),
                ("SEQ_1","XXXX"),
                ("SEQ_2","2020-XX"),
                ("SEQ_3","2020-03-XX"),
                ("SEQ_4","2020-03-02"),
                ("SEQ_5","?")]
        args = get_valid_args(data, tmpdir)
        args.exclude_ambiguous_dates_by = 'day'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain, {FILTER_REASON_KWARGS_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_ambiguous_date'
        """)
        assert results == [
            ('SEQ_1', '[["ambiguity", "day"]]'),
            ('SEQ_2', '[["ambiguity", "day"]]'),
            ('SEQ_3', '[["ambiguity", "day"]]'),
            ('SEQ_5', '[["ambiguity", "day"]]')
        ]

    def test_filter_by_ambiguous_date_any(self, tmpdir):
        """Filter out dates with any ambiguity."""
        data = [("strain","date"),
                ("SEQ_1","XXXX"),
                ("SEQ_2","2020-XX"),
                ("SEQ_3","2020-03-XX"),
                ("SEQ_4","2020-03-02"),
                ("SEQ_5","?")]
        args = get_valid_args(data, tmpdir)
        args.exclude_ambiguous_dates_by = 'any'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain, {FILTER_REASON_KWARGS_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_ambiguous_date'
        """)
        assert results == [
            ('SEQ_1', '[["ambiguity", "any"]]'),
            ('SEQ_2', '[["ambiguity", "any"]]'),
            ('SEQ_3', '[["ambiguity", "any"]]'),
            ('SEQ_5', '[["ambiguity", "any"]]')
        ]

    def test_filter_by_exclude_where(self, tmpdir):
        """Filter by an expression that matches location equal to colorado."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        args.exclude_where = ["location=colorado"]
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_exclude_where'
        """)
        assert results == [("SEQ_1",), ("SEQ_2",)]

    def test_filter_by_exclude_where_missing_column_error(self, tmpdir):
        """Try filtering by an expression matching on an invalid column."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        args.exclude_where = ["invalid=colorado"]
        with pytest.raises(FilterException) as e_info:
            get_filter_obj_run(args)
        assert str(e_info.value) == 'no such column: metadata.invalid'

    def test_force_include_where_missing_column_error(self, tmpdir):
        """Try filtering by an expression matching on an invalid column."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        args.include_where = ["invalid=colorado"]
        with pytest.raises(FilterException) as e_info:
            get_filter_obj_run(args)
        assert str(e_info.value) == 'no such column: metadata.invalid'

    def test_filter_by_min_length(self, tmpdir):
        """Filter by minimum sequence length of 3."""
        data = [("strain",),
                ("SEQ_1",),
                ("SEQ_2",),
                ("SEQ_3",)]
        args = get_valid_args(data, tmpdir)
        fasta_lines = [
            ">SEQ_1", "aa",
            ">SEQ_2", "aaa",
            ">SEQ_3", "nnnn",
        ]
        args.sequences = write_file(tmpdir, "sequences.fasta", "\n".join(fasta_lines))
        args.min_length = 3
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_sequence_length'
        """)
        assert results == [("SEQ_1",), ("SEQ_3",)]

    def test_filter_by_non_nucleotide(self, tmpdir):
        """Filter out sequences with at least 1 invalid nucleotide character."""
        data = [("strain",),
                ("SEQ_1",),
                ("SEQ_2",),
                ("SEQ_3",),
                ("SEQ_4",)]
        args = get_valid_args(data, tmpdir)
        fasta_lines = [
            ">SEQ_1", "aaaa",
            ">SEQ_2", "nnnn",
            ">SEQ_3", "xxxx",
            ">SEQ_4", "aaax",
        ]
        args.sequences = write_file(tmpdir, "sequences.fasta", "\n".join(fasta_lines))
        args.non_nucleotide = True
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_non_nucleotide'
        """)
        assert results == [("SEQ_3",), ("SEQ_4",)]

    def test_filter_by_exclude_all(self, tmpdir):
        """Filter out all sequences."""
        data = [("strain",),
                ("SEQ_1",),
                ("SEQ_2",),
                ("SEQ_3",),
                ("SEQ_4",)]
        args = get_valid_args(data, tmpdir)
        args.exclude_all = True
        with pytest.raises(FilterException) as e_info:
            get_filter_obj_run(args)
        assert str(e_info.value) == "All samples have been dropped! Check filter rules and metadata file format."

    def test_filter_by_exclude_strains(self, tmpdir):
        """Exclude strains from a file."""
        data = [("strain",),
                ("SEQ_1",),
                ("SEQ_2",),
                ("SEQ_3",),
                ("SEQ_4",)]
        args = get_valid_args(data, tmpdir)
        exclude_seqs = [
            "SEQ_1",
            "SEQ_3",
        ]
        args.exclude = [write_file(tmpdir, "exclude.txt", "\n".join(exclude_seqs))]
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_exclude_strains'
        """)
        assert results == [("SEQ_1",), ("SEQ_3",)]

    def test_filter_by_force_include_strains(self, tmpdir):
        """Force-include strains from a file."""
        data = [("strain",),
                ("SEQ_1",),
                ("SEQ_2",),
                ("SEQ_3",),
                ("SEQ_4",)]
        args = get_valid_args(data, tmpdir)
        include_seqs = [
            "SEQ_1",
            "SEQ_3",
        ]
        args.include = [write_file(tmpdir, "include.txt", "\n".join(include_seqs))]
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'force_include_strains'
        """)
        assert results == [("SEQ_1",), ("SEQ_3",)]

    def test_filter_by_exclude_where_negative(self, tmpdir):
        """Filter by an expression that matches location not equal to colorado."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        args.exclude_where = ["location!=colorado"]
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain, {FILTER_REASON_KWARGS_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_exclude_where'
        """)
        assert results == [('SEQ_3', '[["exclude_where", "location!=colorado"]]')]
