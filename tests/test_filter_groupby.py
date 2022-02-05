import pytest
from augur.filter_support.db.base import FilterException
from augur.filter_support.db.sqlite import (
    DEFAULT_DATE_COL,
    FILTER_REASON_COL,
    METADATA_FILTER_REASON_TABLE_NAME,
    METADATA_TABLE_NAME,
    STRAIN_COL,
    FilterSQLite
)
from test_filter import parse_args, write_metadata


@pytest.fixture(scope='function')
def filter_obj_with_metadata(tmpdir):
    """Returns a filter object connected to an in-memory database per function."""
    obj = FilterSQLite(':memory:')
    obj.db_connect()
    data = [
        (STRAIN_COL, DEFAULT_DATE_COL, 'country'),
        ("SEQ_1","2020-01-XX","A"),
        ("SEQ_2","2020-02-01","A"),
        ("SEQ_3","2020-03-01","B"),
        ("SEQ_4","2020-04-01","B"),
        ("SEQ_5","2020-05-01","B")
    ]
    meta_fn = write_metadata(tmpdir, data)
    args = parse_args(f'--metadata {meta_fn}')
    obj.set_args(args)
    obj.db_load_metadata()
    obj.add_attributes()
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

    def test_filter_groupby_skip_ambiguous_month(self, filter_obj_with_metadata:FilterSQLite):
        # modify SEQ_2 to have ambiguous month
        filter_obj_with_metadata.cur.execute(f"""
            UPDATE {METADATA_TABLE_NAME}
            SET {DEFAULT_DATE_COL} = '2020-XX-01'
            WHERE {STRAIN_COL} = 'SEQ_2'
        """)
        filter_obj_with_metadata.connection.commit()
        # add arguments for subsampling
        filter_obj_with_metadata.args.group_by = ['country', 'year', 'month']
        # set up database
        filter_obj_with_metadata.db_create_date_table()
        filter_obj_with_metadata.include_exclude_filter()
        # check filter reasons
        filter_obj_with_metadata.cur.execute(f"""
            SELECT {STRAIN_COL} FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'skip_group_by_with_ambiguous_month'
        """)
        results = filter_obj_with_metadata.cur.fetchall()
        assert results == [('SEQ_2',)]

    def test_filter_groupby_skip_missing_month(self, filter_obj_with_metadata:FilterSQLite):
        # modify SEQ_2 to have year only
        filter_obj_with_metadata.cur.execute(f"""
            UPDATE {METADATA_TABLE_NAME}
            SET {DEFAULT_DATE_COL} = '2020'
            WHERE {STRAIN_COL} = 'SEQ_2'
        """)
        filter_obj_with_metadata.connection.commit()
        # add arguments for subsampling
        filter_obj_with_metadata.args.group_by = ['country', 'year', 'month']
        # set up database
        filter_obj_with_metadata.db_create_date_table()
        filter_obj_with_metadata.include_exclude_filter()
        # check filter reasons
        filter_obj_with_metadata.cur.execute(f"""
            SELECT {STRAIN_COL} FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'skip_group_by_with_ambiguous_month'
        """)
        results = filter_obj_with_metadata.cur.fetchall()
        assert results == [('SEQ_2',)]
