import argparse
import shlex
import sqlite3

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


def get_filter_obj_run(args:argparse.Namespace):
    """Returns a filter object connected to an in-memory database with run() invoked."""
    # use an in-memory database for tests since:
    # 1. test data is not large
    # 2. in-memory I/O is generally faster
    obj = FilterSQLite(in_memory_db=True)
    obj.set_args(args)
    obj.run()
    return obj


def get_valid_args(data, tmpdir):
    """Returns an argparse.Namespace with metadata and output_strains"""
    meta_fn = write_metadata(tmpdir, data)
    return parse_args(f'--metadata {meta_fn} --output-strains {tmpdir / "strains.txt"}')


def query_fetchall(filter_obj:FilterSQLite, query:str):
    with filter_obj.get_db_context() as con:
        return con.execute(query).fetchall()


def query_fetchall_dict(filter_obj:FilterSQLite, query:str):
    filter_obj.connection.row_factory = sqlite3.Row
    with filter_obj.get_db_context() as con:
        return [dict(row) for row in con.execute(query)]
