import pytest
from augur.filter_support.db.base import FilterException
from augur.filter_support.db.sqlite import FilterSQLite
from test_filter import argparser, write_metadata


@pytest.fixture(scope='function')
def filter_obj_with_metadata(tmpdir, argparser):
    """Returns a filter object connected to an in-memory database per function."""
    obj = FilterSQLite(':memory:')
    obj.db_connect()
    data = [
        ('strain','date','country'),
        ("SEQ_1","2020-01-XX","A"),
        ("SEQ_2","2020-02-01","A"),
        ("SEQ_3","2020-03-01","B"),
        ("SEQ_4","2020-04-01","B"),
        ("SEQ_5","2020-05-01","B")
    ]
    meta_fn = write_metadata(tmpdir, data)
    args = argparser(f'--metadata {meta_fn}')
    obj.set_args(args)
    obj.db_load_metadata()
    return obj


class TestFilterGroupBy:
    def test_filter_groupby_invalid_error(self, filter_obj_with_metadata:FilterSQLite):
        groups = ['invalid']
        with pytest.raises(FilterException) as e_info:
            filter_obj_with_metadata.get_valid_group_by_cols(groups)
        assert str(e_info.value) == "The specified group-by categories (['invalid']) were not found. No sequences-per-group sampling will be done."

    def test_filter_groupby_invalid_warn(self, filter_obj_with_metadata:FilterSQLite, capsys):
        groups = ['country', 'year', 'month', 'invalid']
        valid_group_by_cols = filter_obj_with_metadata.get_valid_group_by_cols(groups)
        assert valid_group_by_cols == ['country', 'year', 'month']
        captured = capsys.readouterr()
        assert captured.err == "WARNING: Some of the specified group-by categories couldn't be found: invalid\nFiltering by group may behave differently than expected!\n"
