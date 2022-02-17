import re
from datetime import date


class InvalidDateFormat(ValueError):
    pass


def date_type(date_in):
    date_in = str(date_in)
    if not valid_date(date_in):
        raise InvalidDateFormat(f'Invalid date format: {date_in}')
    return date_in


def valid_date(date_in):
    date_in = str(date_in)
    # ISO 8601 date
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_in):
        return True
    # int, negative ok
    if re.match(r'^-*\d+$', date_in):
        return True
    # float, negative ok
    if re.match(r'^-*\d+\.\d+$', date_in):
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


# TODO: DateDisambiguator parity
# assert_only_less_significant_uncertainty

def get_date_min(date_in):
    """Get the minimum date from a potentially ambiguous date."""
    date_in = str(date_in)
    if not date_in:
        return None
    if re.match(r'^-*\d+\.\d+$', date_in):
        # date is a numeric date
        # can be negative
        # year-only is ambiguous
        return float(date_in)
    # convert to numeric
    date_parts = date_in.split('-', maxsplit=2)
    try:
        year = int(date_parts[0].replace('X', '0'))
        month = int(date_parts[1]) if len(date_parts) > 1 and date_parts[1].isnumeric() else 1
        day = int(date_parts[2]) if len(date_parts) > 2 and date_parts[2].isnumeric() else 1
        return date_to_numeric_cached(date(year, month, day))
    except ValueError:
        return None


def get_date_max(date_in):
    """Get the maximum date from a potentially ambiguous date."""
    date_in = str(date_in)
    if not date_in:
        return None
    if re.match(r'^-*\d+\.\d+$', date_in):
        # date is a numeric date
        # can be negative
        # year-only is ambiguous
        return float(date_in)
    # convert to numeric
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
        return date_to_numeric_cached(date(year, month, day))
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

cache = dict()
def date_to_numeric_cached(d:date):
    """Return the numeric date representation of a datetime.date."""
    if d not in cache:
        cache[d] = date_to_numeric(d)
    if cache[d] > today_numeric:
        cache[d] = today_numeric
    return cache[d]
