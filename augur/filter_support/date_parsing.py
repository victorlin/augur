import re
from datetime import date
from functools import lru_cache
from typing import List


class InvalidDateFormat(ValueError):
    pass

RE_ISO_8601_DATE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# non-negative int (year-only date)
RE_YEAR_ONLY = re.compile(r'^\d+$')

# float, negative ok
# year-only is ambiguous
RE_NUMERIC_DATE = re.compile(r'^-*\d+\.\d+$')


def date_type(date_in):
    date_in = str(date_in)
    if not valid_date(date_in):
        raise InvalidDateFormat(f'Invalid date format: {date_in}')
    return date_in


def valid_date(date_in):
    date_in = str(date_in)
    if RE_ISO_8601_DATE.match(date_in):
        return True
    if RE_YEAR_ONLY.match(date_in):
        return True
    if RE_NUMERIC_DATE.match(date_in):
        return True
    return False


def get_year(date_in):
    """Get the year from a date. Only works for ISO dates."""
    date_in = str(date_in)
    try:
        return int(date_in.split('-')[0])
    except:
        return None


def get_month(date_in):
    """Get the month from a date. Only works for ISO dates."""
    date_in = str(date_in)
    try:
        return int(date_in.split('-')[1])
    except:
        return None


def get_day(date_in):
    """Get the day from a date. Only works for ISO dates."""
    date_in = str(date_in)
    try:
        return int(date_in.split('-')[2])
    except:
        return None


ASSERT_ONLY_LESS_SIGNIFICANT_AMBIGUITY_VALUE = 'assert_only_less_significant_ambiguity'


def assert_only_less_significant_ambiguity(date_parts:List[str]):
    has_exact_year = date_parts[0].isnumeric()
    has_exact_month = len(date_parts) > 1 and date_parts[1].isnumeric()
    has_exact_day = len(date_parts) > 2 and date_parts[2].isnumeric()
    if has_exact_day and not (has_exact_month and has_exact_year):
        raise InvalidDateFormat(ASSERT_ONLY_LESS_SIGNIFICANT_AMBIGUITY_VALUE)
    if has_exact_month and not has_exact_year:
        raise InvalidDateFormat(ASSERT_ONLY_LESS_SIGNIFICANT_AMBIGUITY_VALUE)


@lru_cache()
def get_date_min(date_in):
    """Get the minimum date from a potentially ambiguous date.

    Also check for assert_only_less_significant_ambiguity.
    If the check raises an :class:`InvalidDateFormat`, return a constant string `ASSERT_ONLY_LESS_SIGNIFICANT_AMBIGUITY_VALUE`
    which comes from the exception.
    """
    date_in = str(date_in)
    if not date_in:
        return None
    if RE_NUMERIC_DATE.match(date_in):
        return float(date_in)
    # convert non-negative ISO to numeric
    is_negative = date_in[0] == '-'
    if is_negative:
        return None
    date_parts = date_in.split('-', maxsplit=2)
    try:
        assert_only_less_significant_ambiguity(date_parts)
    except InvalidDateFormat as e:
        return str(e)
    try:
        year = int(date_parts[0].replace('X', '0'))
        month = int(date_parts[1]) if len(date_parts) > 1 and date_parts[1].isnumeric() else 1
        day = int(date_parts[2]) if len(date_parts) > 2 and date_parts[2].isnumeric() else 1
        return date_to_numeric_capped(date(year, month, day))
    except ValueError:
        return None


@lru_cache()
def get_date_max(date_in):
    """Get the maximum date from a potentially ambiguous date."""
    date_in = str(date_in)
    if not date_in:
        return None
    if RE_NUMERIC_DATE.match(date_in):
        return float(date_in)
    # convert non-negative ISO to numeric
    is_negative = date_in[0] == '-'
    if is_negative:
        return None
    date_parts = date_in.split('-', maxsplit=2)
    try:
        year = int(date_parts[0].replace('X', '9'))
        month = int(date_parts[1]) if len(date_parts) > 1 and date_parts[1].isnumeric() else 12
        if len(date_parts) == 3 and date_parts[2].isnumeric():
            day = int(date_parts[2])
        else:
            if month in {1,3,5,7,8,10,12}:
                day = 31
            elif month == 2:
                day = 28
            else:
                day = 30
        return date_to_numeric_capped(date(year, month, day))
    except ValueError:
        return None


### date_to_numeric logic ###
# copied from treetime.utils.numeric_date
# simplified+cached for speed

from calendar import isleap
def date_to_numeric(d:date):
    days_in_year = 366 if isleap(d.year) else 365
    return d.year + (d.timetuple().tm_yday-0.5) / days_in_year

today_numeric = date_to_numeric(date.today())

@lru_cache()
def date_to_numeric_capped(d:date):
    """Return the numeric date representation of a datetime.date."""
    d_numeric = date_to_numeric(d)
    if d_numeric > today_numeric:
        d_numeric = today_numeric
    return d_numeric
