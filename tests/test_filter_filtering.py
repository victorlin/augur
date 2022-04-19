import pytest
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
    get_filter_obj_run,
    get_valid_args,
    query_fetchall,
    write_file,
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
