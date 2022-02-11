import pytest

from augur.filter_support.db.sqlite import (
    DATE_TABLE_NAME,
)

from tests.test_filter import get_filter_obj_run, get_valid_args, query_fetchall_dict


class TestDateParsing:
    def test_valid_ambiguous_day(self, tmpdir):
        data = [
            ('strain','date','country'),
            ("SEQ_1","2020-01-XX","A"),
        ]
        args = get_valid_args(data, tmpdir)
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall_dict(filter_obj, f"""
            SELECT date_min, date_max FROM {DATE_TABLE_NAME}
        """)
        assert results[0]['date_min'] == pytest.approx(2020.0013661202186)
        assert results[0]['date_max'] == pytest.approx(2020.0833333333333)
