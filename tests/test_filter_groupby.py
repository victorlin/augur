import pytest
from augur.filter_support.exceptions import FilterException
from augur.filter_support.subsample import get_valid_group_by_cols
from augur.filter_support.db.sqlite import (
    DEFAULT_DATE_COL,
    FILTER_REASON_COL,
    METADATA_FILTER_REASON_TABLE_NAME,
    STRAIN_COL,
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
        assert captured.err == "WARNING: Some of the specified group-by categories couldn't be found: invalid\nFiltering by group may behave differently than expected!\n"

    def test_filter_groupby_skip_ambiguous_month(self, tmpdir):
        data = [
            (STRAIN_COL, DEFAULT_DATE_COL, 'country'),
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
            SELECT {STRAIN_COL} FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'skip_group_by_with_ambiguous_month'
        """)
        assert results == [('SEQ_2',)]

    def test_filter_groupby_skip_missing_month(self, tmpdir):
        data = [
            (STRAIN_COL, DEFAULT_DATE_COL, 'country'),
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
            SELECT {STRAIN_COL} FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'skip_group_by_with_ambiguous_month'
        """)
        assert results == [('SEQ_2',)]
