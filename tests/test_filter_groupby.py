import pytest
from textwrap import dedent
from augur.filter_support.exceptions import FilterException
from augur.filter_support.subsample import get_valid_group_by_cols
from augur.filter_support.db.sqlite import (
    FILTER_REASON_COL,
    GROUP_SIZES_TABLE_NAME,
    METADATA_FILTER_REASON_TABLE_NAME,
)
from test_filter import (
    get_filter_obj_run,
    get_valid_args,
    query_fetchall,
)


class TestFilterGroupBy:
    def test_filter_groupby_invalid_error(self):
        metadata_cols = {'strain', 'date', 'country'}
        groups = ['invalid']
        with pytest.raises(FilterException) as e_info:
            get_valid_group_by_cols(groups, metadata_cols)
        assert str(e_info.value) == "The specified group-by categories (['invalid']) were not found. No sequences-per-group sampling will be done."

    def test_filter_groupby_invalid_warn(self, capsys):
        metadata_cols = {'strain', 'date', 'country'}
        groups = ['country', 'year', 'month', 'invalid']
        valid_group_by_cols = get_valid_group_by_cols(groups, metadata_cols)
        assert valid_group_by_cols == ['country', 'year', 'month']
        captured = capsys.readouterr()
        assert captured.err == dedent("""\
            WARNING: Some of the specified group-by categories couldn't be found: invalid
            Filtering by group may behave differently than expected!
        """)

    def test_filter_groupby_skip_ambiguous_year(self, tmpdir):
        data = [
            ('strain','date','country'),
            ("SEQ_1","2020-01-XX","A"),
            ("SEQ_2","","A"),
            ("SEQ_3","2020-03-01","B"),
            ("SEQ_4","2020-04-01","B"),
            ("SEQ_5","2020-05-01","B")
        ]
        args = get_valid_args(data, tmpdir)
        args.group_by = ['country', 'year', 'month']
        args.sequences_per_group = 1
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'skip_group_by_with_ambiguous_year'
        """)
        assert results == [('SEQ_2',)]

    def test_filter_groupby_skip_missing_date(self, tmpdir):
        data = [
            ('strain','date','country'),
            ("SEQ_1","2020-01-XX","A"),
            ("SEQ_2","","A"),
            ("SEQ_3","2020-03-01","B"),
            ("SEQ_4","2020-04-01","B"),
            ("SEQ_5","2020-05-01","B")
        ]
        args = get_valid_args(data, tmpdir)
        args.group_by = ['country', 'year', 'month']
        args.sequences_per_group = 1
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'skip_group_by_with_ambiguous_year'
        """)
        assert results == [('SEQ_2',)]

    def test_filter_groupby_skip_ambiguous_month(self, tmpdir):
        data = [
            ('strain','date','country'),
            ("SEQ_1","2020-01-XX","A"),
            ("SEQ_2","2020-XX-01","A"),
            ("SEQ_3","2020-03-01","B"),
            ("SEQ_4","2020-04-01","B"),
            ("SEQ_5","2020-05-01","B")
        ]
        args = get_valid_args(data, tmpdir)
        args.group_by = ['country', 'year', 'month']
        args.sequences_per_group = 1
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'skip_group_by_with_ambiguous_month'
        """)
        assert results == [('SEQ_2',)]

    def test_filter_groupby_skip_missing_month(self, tmpdir):
        data = [
            ('strain','date','country'),
            ("SEQ_1","2020-01-XX","A"),
            ("SEQ_2","2020","A"),
            ("SEQ_3","2020-03-01","B"),
            ("SEQ_4","2020-04-01","B"),
            ("SEQ_5","2020-05-01","B")
        ]
        args = get_valid_args(data, tmpdir)
        args.group_by = ['country', 'year', 'month']
        args.sequences_per_group = 1
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'skip_group_by_with_ambiguous_month'
        """)
        assert results == [('SEQ_2',)]

    def test_filter_groupby_missing_year_error(self):
        metadata_cols = {'strain', 'country'}
        groups = ['year']
        with pytest.raises(FilterException) as e_info:
            get_valid_group_by_cols(groups, metadata_cols)
        assert str(e_info.value) == "The specified group-by categories (['year']) were not found. No sequences-per-group sampling will be done. Note that using 'year' or 'year month' requires a column called 'date'."

    def test_filter_groupby_missing_month_error(self):
        metadata_cols = {'strain', 'country'}
        groups = ['month']
        with pytest.raises(FilterException) as e_info:
            get_valid_group_by_cols(groups, metadata_cols)
        assert str(e_info.value) == "The specified group-by categories (['month']) were not found. No sequences-per-group sampling will be done. Note that using 'year' or 'year month' requires a column called 'date'."

    def test_filter_groupby_missing_year_and_month_error(self):
        metadata_cols = {'strain', 'country'}
        groups = ['year', 'month']
        with pytest.raises(FilterException) as e_info:
            get_valid_group_by_cols(groups, metadata_cols)
        assert str(e_info.value) == "The specified group-by categories (['year', 'month']) were not found. No sequences-per-group sampling will be done. Note that using 'year' or 'year month' requires a column called 'date'."

    def test_filter_groupby_missing_date_warn(self, capsys):
        metadata_cols = {'strain', 'country'}
        groups = ['country', 'year', 'month']
        valid_group_by_cols = get_valid_group_by_cols(groups, metadata_cols)
        assert valid_group_by_cols == ['country']
        captured = capsys.readouterr()
        assert captured.err == dedent("""\
            WARNING: A 'date' column could not be found to group-by year.
            WARNING: A 'date' column could not be found to group-by month.
            WARNING: Some of the specified group-by categories couldn't be found: year, month
            Filtering by group may behave differently than expected!
        """)

    def test_filter_groupby_only_year_provided(self, tmpdir):
        data = [
            ('strain','date','country'),
            ("SEQ_1","2020-01-XX","A"),
            ("SEQ_2","2020","B"),
            ("SEQ_3","2020-03-01","C"),
            ("SEQ_4","2020-04-01","C"),
            ("SEQ_5","2020-05-01","C")
        ]
        args = get_valid_args(data, tmpdir)
        args.group_by = ['country', 'year']
        args.sequences_per_group = 1
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT country, year FROM {GROUP_SIZES_TABLE_NAME}
        """)
        assert results == [
            ('A', 2020),
            ('B', 2020),
            ('C', 2020)
        ]

    def test_filter_groupby_month_with_only_year_provided(self, tmpdir):
        data = [
            ('strain','date','country'),
            ("SEQ_1","2020-01-XX","A"),
            ("SEQ_2","2020","B"),
            ("SEQ_3","2020-03-01","C"),
            ("SEQ_4","2020-04-01","C"),
            ("SEQ_5","2020-05-01","C")
        ]
        args = get_valid_args(data, tmpdir)
        args.group_by = ['country', 'year', 'month']
        args.sequences_per_group = 1
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT country, year, month FROM {GROUP_SIZES_TABLE_NAME}
        """)
        assert results == [
            ('A', 2020, 1),
            ('C', 2020, 3),
            ('C', 2020, 4),
            ('C', 2020, 5)
        ]
        results = query_fetchall(filter_obj, f"""
            SELECT strain FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'skip_group_by_with_ambiguous_month'
        """)
        assert results == [('SEQ_2',)]

    def test_filter_groupby_only_year_month_provided(self, tmpdir):
        data = [
            ('strain','date','country'),
            ("SEQ_1","2020-01","A"),
            ("SEQ_2","2020-02","B"),
            ("SEQ_3","2020-03","C"),
            ("SEQ_4","2020-04","D"),
            ("SEQ_5","2020-05","E")
        ]
        args = get_valid_args(data, tmpdir)
        args.group_by = ['country', 'year', 'month']
        args.sequences_per_group = 1
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT country, year, month FROM {GROUP_SIZES_TABLE_NAME}
        """)
        assert results == [
            ('A', 2020, 1),
            ('B', 2020, 2),
            ('C', 2020, 3),
            ('D', 2020, 4),
            ('E', 2020, 5)
        ]
        results = query_fetchall(filter_obj, f"""
            SELECT strain FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} IS NULL
        """)
        assert results == [('SEQ_1',), ('SEQ_2',), ('SEQ_3',), ('SEQ_4',), ('SEQ_5',)]

    def test_all_samples_dropped(self, tmpdir):
        data = [
            ('strain','date','country'),
            ("SEQ_1","2020","A"),
            ("SEQ_2","2020","B"),
            ("SEQ_3","2020","C"),
            ("SEQ_4","2020","D"),
            ("SEQ_5","2020","E")
        ]
        args = get_valid_args(data, tmpdir)
        args.group_by = ['country', 'year', 'month']
        args.sequences_per_group = 1
        with pytest.raises(FilterException) as e_info:
            get_filter_obj_run(args)
        assert str(e_info.value) == "All samples have been dropped! Check filter rules and metadata file format."
