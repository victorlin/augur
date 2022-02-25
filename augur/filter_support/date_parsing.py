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
    """Validate date format and return string form if valid.

    Intended for use as the `type` parameter in `argparse.ArgumentParser.add_argument()`."""
    date_in = str(date_in)
    if not valid_date(date_in):
        raise InvalidDateFormat(f'Invalid date format: {date_in}')
    return date_in


def valid_date(d:str):
    """Check that a date string is in a supported format."""
    if RE_ISO_8601_DATE.match(d):
        return True
    if RE_YEAR_ONLY.match(d):
        return True
    if RE_NUMERIC_DATE.match(d):
        return True
    return False


CACHE_SIZE = 8192
# The following functions use a cache to minimize redundant operations on
# large datasets that are likely to have multiple entries with the same date value.


@lru_cache(maxsize=CACHE_SIZE)
def get_year(date_in):
    """Get the year from a date. Only works for ISO dates.

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    date_in = str(date_in)
    try:
        return int(date_in.split('-')[0])
    except:
        return None


@lru_cache(maxsize=CACHE_SIZE)
def get_month(date_in):
    """Get the month from a date. Only works for ISO dates.

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    date_in = str(date_in)
    try:
        return int(date_in.split('-')[1])
    except:
        return None


@lru_cache(maxsize=CACHE_SIZE)
def get_day(date_in):
    """Get the day from a date. Only works for ISO dates.

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    date_in = str(date_in)
    try:
        return int(date_in.split('-')[2])
    except:
        return None


@lru_cache(maxsize=CACHE_SIZE)
def get_date_min(date_in):
    """Get the minimum date from a potentially ambiguous date.

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    date_in = str(date_in)
    if not date_in:
        return None
    if RE_NUMERIC_DATE.match(date_in):
        return float(date_in)
    if date_in[0] == '-':
        # negative ISO date not supported
        return None
    date_parts = date_in.split('-', maxsplit=2)
    try:
        # convert ISO to numeric, resolving any ambiguity
        # TODO: resolve partial month/day ambiguity eg. 2018-1X-XX, 2018-10-3X
        year = int(date_parts[0].replace('X', '0'))
        month = int(date_parts[1]) if len(date_parts) > 1 and date_parts[1].isnumeric() else 1
        day = int(date_parts[2]) if len(date_parts) > 2 and date_parts[2].isnumeric() else 1
        return date_to_numeric_capped(date(year, month, day))
    except ValueError:
        return None


@lru_cache(maxsize=CACHE_SIZE)
def get_date_max(date_in):
    """Get the maximum date from a potentially ambiguous date.

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    date_in = str(date_in)
    if not date_in:
        return None
    if RE_NUMERIC_DATE.match(date_in):
        return float(date_in)
    if date_in[0] == '-':
        # negative ISO date not supported
        return None
    date_parts = date_in.split('-', maxsplit=2)
    try:
        # convert ISO to numeric, resolving any ambiguity
        # TODO: resolve partial month/day ambiguity eg. 2018-1X-XX, 2018-10-3X
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


@lru_cache(maxsize=CACHE_SIZE)
def get_date_errors(date_in):
    """Check date for any errors.

    assert_only_less_significant_ambiguity:

    If an exception is raised here, it will result in a `sqlite3.OperationalError`
    without trace to the original exception. For this reason, if the check raises
    :class:`InvalidDateFormat`, return a constant string
    `ASSERT_ONLY_LESS_SIGNIFICANT_AMBIGUITY_VALUE` which sqlite3 can then "handle".

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    date_in = str(date_in)
    if not date_in:
        # let empty string pass silently
        return None
    if RE_NUMERIC_DATE.match(date_in):
        # let numeric dates pass silently
        return None
    if date_in[0] == '-':
        # let negative ISO dates pass silently
        return None
    date_parts = date_in.split('-', maxsplit=2)
    try:
        assert_only_less_significant_ambiguity(date_parts)
    except InvalidDateFormat as e:
        return str(e)


ASSERT_ONLY_LESS_SIGNIFICANT_AMBIGUITY_ERROR = 'assert_only_less_significant_ambiguity'


def assert_only_less_significant_ambiguity(date_parts:List[str]):
    """
    Raise an exception if a constrained digit appears in a less-significant place
    than an uncertain digit.

    These patterns are valid:
        2000-01-01
        2000-01-XX
        2000-XX-XX

    but this is invalid, because month is uncertain but day is constrained:
        2000-XX-01

    These invalid cases are assumed to be unintended use of the tool.
    """
    has_exact_year = date_parts[0].isnumeric()
    has_exact_month = len(date_parts) > 1 and date_parts[1].isnumeric()
    has_exact_day = len(date_parts) > 2 and date_parts[2].isnumeric()
    if has_exact_day and not (has_exact_month and has_exact_year):
        raise InvalidDateFormat(ASSERT_ONLY_LESS_SIGNIFICANT_AMBIGUITY_ERROR)
    if has_exact_month and not has_exact_year:
        raise InvalidDateFormat(ASSERT_ONLY_LESS_SIGNIFICANT_AMBIGUITY_ERROR)



### date_to_numeric logic ###
# copied from treetime.utils.numeric_date
# simplified+cached for speed

from calendar import isleap
def date_to_numeric(d:date):
    """Return the numeric date representation of a datetime.date."""
    days_in_year = 366 if isleap(d.year) else 365
    return d.year + (d.timetuple().tm_yday-0.5) / days_in_year


@lru_cache(maxsize=CACHE_SIZE)
def date_to_numeric_capped(d:date, max_numeric:float=date_to_numeric(date.today())):
    """Return the numeric date representation of a datetime.date, capped at a maximum numeric value."""
    d_numeric = date_to_numeric(d)
    if d_numeric > max_numeric:
        d_numeric = max_numeric
    return d_numeric
