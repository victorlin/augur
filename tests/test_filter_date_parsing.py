import pytest
from treetime.utils import numeric_date
from datetime import date
from augur.filter_support.date_parsing import InvalidDateFormat

from augur.filter_support.db.sqlite import (
    DATE_MIN_COL,
    DATE_MAX_COL,
    DATE_TABLE_NAME,
)

from tests.test_filter import get_filter_obj_run, get_valid_args, query_fetchall


def get_parsed_date_min_max(date:str, tmpdir):
    data = [
        ('strain','date'),
        ("SEQ_1",date),
    ]
    args = get_valid_args(data, tmpdir)
    filter_obj = get_filter_obj_run(args)
    results = query_fetchall(filter_obj, f"""
        SELECT {DATE_MIN_COL}, {DATE_MAX_COL} FROM {DATE_TABLE_NAME}
    """)
    return results[0]


class TestDateParsing:
    def test_ambiguous_day(self, tmpdir):
        """Ambiguous day yields a certain min/max range."""
        date_min, date_max = get_parsed_date_min_max(
                                        '2018-01-XX', tmpdir)
        assert date_min == pytest.approx(2018.001, abs=1e-3)
        assert date_max == pytest.approx(2018.083, abs=1e-3)

    def test_missing_day(self, tmpdir):
        """Date without day yields a range equivalent to ambiguous day."""
        date_min, date_max = get_parsed_date_min_max(
                                        '2018-01', tmpdir)
        assert date_min == pytest.approx(2018.001, abs=1e-3)
        assert date_max == pytest.approx(2018.083, abs=1e-3)

    def test_ambiguous_month(self, tmpdir):
        """Ambiguous month yields a certain min/max range."""
        date_min, date_max = get_parsed_date_min_max(
                                        '2018-XX-XX', tmpdir)
        assert date_min == pytest.approx(2018.001, abs=1e-3)
        assert date_max == pytest.approx(2018.999, abs=1e-3)

    def test_missing_month(self, tmpdir):
        """Date without month/day yields a range equivalent to ambiguous month/day."""
        date_min, date_max = get_parsed_date_min_max(
                                        '2018', tmpdir)
        assert date_min == pytest.approx(2018.001, abs=1e-3)
        assert date_max == pytest.approx(2018.999, abs=1e-3)

    def test_numerical_exact_year(self, tmpdir):
        """Numerical year ending in .0 should be interpreted as exact."""
        date_min, date_max = get_parsed_date_min_max(
                                        '2018.0', tmpdir)
        assert date_min == pytest.approx(2018.001, abs=1e-3)
        assert date_max == pytest.approx(2018.001, abs=1e-3)

    def test_ambiguous_year(self, tmpdir):
        """Ambiguous year replaces X with 0 (min) and 9 (max)."""
        date_min, date_max = get_parsed_date_min_max(
                                        '201X-XX-XX', tmpdir)
        assert date_min == pytest.approx(2010.001, abs=1e-3)
        assert date_max == pytest.approx(2019.999, abs=1e-3)

    def test_ambiguous_year_incomplete_date(self, tmpdir):
        """Ambiguous year without month/day yields a range equivalent to ambiguous month/day counterpart."""
        date_min, date_max = get_parsed_date_min_max(
                                        '201X', tmpdir)
        assert date_min == pytest.approx(2010.001, abs=1e-3)
        assert date_max == pytest.approx(2019.999, abs=1e-3)

    def test_ambiguous_year_decade(self, tmpdir):
        """Parse year-only ambiguous date with ambiguous decade."""
        date_min, date_max = get_parsed_date_min_max(
                                        '10X1', tmpdir)
        assert date_min == pytest.approx(1001.001, abs=1e-3)
        assert date_max == pytest.approx(1091.999, abs=1e-3)

    def test_ambiguous_year_incomplete_date(self, tmpdir):
        """Ambiguous year without explicit X fails parsing."""
        with pytest.raises(InvalidDateFormat) as e_info:
            get_parsed_date_min_max('201x', tmpdir)
        assert str(e_info.value) == "Some dates have an invalid format (showing at most 3): '201x'"

    # TODO: DateDisambiguator parity: max_date = min(max_date, datetime.date.today())
    @pytest.mark.skip(reason="not implemented")
    def test_future_year(self, tmpdir):
        """Date from the future should be converted to today."""
        date_min, date_max = get_parsed_date_min_max(
                                        '3000', tmpdir)
        assert date_min == pytest.approx(numeric_date(date.today()), abs=1e-3)
        assert date_max == pytest.approx(numeric_date(date.today()), abs=1e-3)

    # TODO: DateDisambiguator parity: assert_only_less_significant_uncertainty
    @pytest.mark.skip(reason="not implemented")
    def test_a(self, tmpdir):
        """Date from the future should be converted to today."""
        get_parsed_date_min_max('2018-XX-01', tmpdir)
        # should raise "Invalid date: Month contains uncertainty, so day must also be uncertain."
