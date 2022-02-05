import argparse
import shlex
import pytest
import augur.filter

from augur.filter_support.db.sqlite import FilterSQLite


def parse_args(args:str):
    parser = argparse.ArgumentParser()
    augur.filter.register_arguments(parser)
    return parser.parse_args(shlex.split(args))


def write_file(tmpdir, filename:str, content:str):
    filepath = str(tmpdir / filename)
    with open(filepath, "w") as handle:
        handle.write(content)
    return filepath


def write_metadata(tmpdir, metadata):
    content = "\n".join(("\t".join(md) for md in metadata))
    return write_file(tmpdir, "metadata.tsv", content)


@pytest.fixture(scope='function')
def filter_obj():
    """Returns a filter object connected to an in-memory database per function."""
    obj = FilterSQLite(':memory:')
    obj.db_connect()
    return obj
