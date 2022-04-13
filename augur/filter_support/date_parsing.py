import argparse
import re
from datetime import date
from functools import lru_cache
from typing import Any, List


class InvalidDateFormat(ValueError):
    pass


# float, negative ok
# note: year-only is treated as incomplete ambiguous and must be non-negative (see RE_YEAR_ONLY)
RE_NUMERIC_DATE = re.compile(r'^-*\d+\.\d+$')

# complete ISO 8601 date
# e.g. 2018-03-25
RE_ISO_8601_DATE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# complete ambiguous ISO 8601 date
# e.g. 2018-03-XX
RE_AMBIGUOUS_ISO_8601_DATE = re.compile(r'^[\dX]{4}-[\dX]{2}-[\dX]{2}$')

# incomplete ambiguous ISO 8601 date (missing day)
# e.g. 2018-03
RE_AMBIGUOUS_ISO_8601_DATE_YEAR_MONTH = re.compile(r'^[\dX]{4}-[\dX]{2}$')

# incomplete ambiguous ISO 8601 date (missing month and day)
# e.g. 2018
# and other non-negative ints
# e.g. 0, 1, 123, 12345
RE_YEAR_ONLY = re.compile(r'^[\dX]+$')

# TODO: relative dates (ISO 8601 durations)
# no regex for ISO 8601 duration (it is complex), just try evaluating last and catch exceptions.


CACHE_SIZE = 8192
# Some functions below use @lru_cache to minimize redundant operations on
# large datasets that are likely to have multiple entries with the same date value.


@lru_cache(maxsize=CACHE_SIZE)
def get_year(date_in:Any):
    """Get the year from a date. Only works for ISO dates.

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    date_in = str(date_in)
    try:
        return int(date_in.split('-')[0])
    except (ValueError, IndexError):
        return None


@lru_cache(maxsize=CACHE_SIZE)
def get_month(date_in:Any):
    """Get the month from a date. Only works for ISO dates.

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    date_in = str(date_in)
    try:
        return int(date_in.split('-')[1])
    except (ValueError, IndexError):
        return None


@lru_cache(maxsize=CACHE_SIZE)
def get_day(date_in:Any):
    """Get the day from a date. Only works for ISO dates.

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    date_in = str(date_in)
    try:
        return int(date_in.split('-')[2])
    except (ValueError, IndexError):
        return None


def iso_to_numeric(date_in:str, ambiguity_resolver:str):
    """Convert ISO 8601 date string to numeric, resolving any ambiguity detected by explicit 'X' characters or missing date parts."""
    date_parts = date_in.split('-', maxsplit=2)
    # TODO: resolve partial month/day ambiguity eg. 2018-1X-XX, 2018-10-3X
    if ambiguity_resolver == 'min':
        year = int(date_parts[0].replace('X', '0'))
        month = int(date_parts[1]) if len(date_parts) > 1 and date_parts[1].isnumeric() else 1
        day = int(date_parts[2]) if len(date_parts) > 2 and date_parts[2].isnumeric() else 1
        return date_to_numeric_capped(date(year, month, day))
    if ambiguity_resolver == 'max':
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


def any_to_numeric(date_in:Any, ambiguity_resolver:str):
    """Return numeric date if date is in a supported format.

    For ambiguous ISO 8601 dates, resolve to either minimum or maximum possible value.
    """
    date_in = str(date_in)
    if RE_NUMERIC_DATE.match(date_in):
        return float(date_in)
    if (RE_ISO_8601_DATE.match(date_in) or
        RE_AMBIGUOUS_ISO_8601_DATE.match(date_in) or
        RE_AMBIGUOUS_ISO_8601_DATE_YEAR_MONTH.match(date_in) or
        RE_YEAR_ONLY.match(date_in)
        ):
        return iso_to_numeric(date_in, ambiguity_resolver)
    raise InvalidDateFormat("TODO")


def any_to_numeric_type_min(date_in:Any):
    """Get the numeric date from any supported date format, taking the minimum possible value if ambiguous.

    This function is intended to be used as the `type` parameter in `argparse.ArgumentParser.add_argument()`

    This raises an ArgumentTypeError from InvalidDateFormat exceptions, otherwise the custom exception message won't be shown in console output due to:
    https://github.com/python/cpython/blob/5c4d1f6e0e192653560ae2941a6677fbf4fbd1f2/Lib/argparse.py#L2503-L2513
    """
    try:
        return any_to_numeric(date_in, ambiguity_resolver='min')
    except InvalidDateFormat as e:
        raise argparse.ArgumentTypeError(str(e)) from e


def any_to_numeric_type_max(date_in:Any):
    """Get the numeric date from any supported date format, taking the maximum possible value if ambiguous.

    This function is intended to be used as the `type` parameter in `argparse.ArgumentParser.add_argument()`

    This raises an ArgumentTypeError from InvalidDateFormat exceptions, otherwise the custom exception message won't be shown in console output due to:
    https://github.com/python/cpython/blob/5c4d1f6e0e192653560ae2941a6677fbf4fbd1f2/Lib/argparse.py#L2503-L2513
    """
    try:
        return any_to_numeric(date_in, ambiguity_resolver='max')
    except InvalidDateFormat as e:
        raise argparse.ArgumentTypeError(str(e)) from e


@lru_cache(maxsize=CACHE_SIZE)
def try_get_numeric_date_min(date_in:Any):
    """Get the numeric date from any supported date format, taking the minimum possible value if ambiguous.

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    try:
        return any_to_numeric(date_in, ambiguity_resolver='min')
    except ValueError:
        return None


@lru_cache(maxsize=CACHE_SIZE)
def try_get_numeric_date_max(date_in:Any):
    """Get the numeric date from any supported date format, taking the maximum possible value if ambiguous.

    This function is intended to be registered as a user-defined function in sqlite3.
    """
    try:
        return any_to_numeric(date_in, ambiguity_resolver='max')
    except ValueError:
        return None


@lru_cache(maxsize=CACHE_SIZE)
def get_date_errors(date_in:Any):
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
