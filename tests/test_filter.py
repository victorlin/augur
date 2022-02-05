import argparse
import shlex
from xml.etree.ElementInclude import include
import pytest
import augur.filter

from augur.filter_support.db.sqlite import (
    EXCLUDE_COL,
    FILTER_REASON_COL,
    INCLUDE_COL,
    METADATA_FILTER_REASON_TABLE_NAME,
    METADATA_TABLE_NAME,
    PRIORITIES_TABLE_NAME,
    STRAIN_COL,
    FilterSQLite
)


@pytest.fixture
def argparser():
    parser = argparse.ArgumentParser()
    augur.filter.register_arguments(parser)
    def parse(args):
        return parser.parse_args(shlex.split(args))
    return parse


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


def get_filter_obj_run(args:argparse.Namespace):
    """Returns a filter object connected to an in-memory database."""
    obj = FilterSQLite(':memory:')
    obj.set_args(args)
    obj.run(cleanup=False)
    return obj


def get_valid_args(data, tmpdir, argparser):
    """Returns an argparse.Namespace with metadata and output_strains"""
    meta_fn = write_metadata(tmpdir, data)
    return argparser(f'--metadata {meta_fn} --output-strains {tmpdir / "strains.txt"}')


def query_fetchall(filter_obj:FilterSQLite, query:str):
    filter_obj.cur.execute(query)
    return filter_obj.cur.fetchall()


class TestFilter:
    def test_load_metadata(self, tmpdir, argparser, filter_obj:FilterSQLite):
        """Load a metadata file."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        meta_fn = write_metadata(tmpdir, data)
        args = argparser(f'--metadata {meta_fn}')
        filter_obj.set_args(args)
        filter_obj.db_load_metadata()
        filter_obj.cur.execute(f"SELECT * FROM {METADATA_TABLE_NAME}")
        table = filter_obj.cur.fetchall()
        assert [row[1:] for row in table] == data[1:]

    def test_load_priority_scores_valid(self, tmpdir, argparser, filter_obj:FilterSQLite):
        """Load a priority score file."""
        content = "strain1\t5\nstrain2\t6\nstrain3\t8\n"
        priorities_fn = write_file(tmpdir, "priorities.txt", content)
        # --metadata is required but we don't need it
        args = argparser(f'--metadata "" --priority {priorities_fn}')
        filter_obj.set_args(args)
        filter_obj.db_load_priorities_table()
        filter_obj.cur.execute(f"SELECT * FROM {PRIORITIES_TABLE_NAME}")
        table = filter_obj.cur.fetchall()
        assert table == [(0, 'strain1', 5.0), (1, 'strain2', 6.0), (2, 'strain3', 8.0)]

    def test_load_priority_scores_malformed(self, tmpdir, argparser, filter_obj:FilterSQLite):
        """Attempt to load a priority score file with non-float in priority column raises a ValueError."""
        content = "strain1 X\n"
        priorities_fn = write_file(tmpdir, "priorities.txt", content)
        # --metadata is required but we don't need it
        args = argparser(f'--metadata "" --priority {priorities_fn}')
        filter_obj.set_args(args)
        with pytest.raises(ValueError) as e_info:
            filter_obj.db_load_priorities_table()
        assert str(e_info.value) == "Failed to parse priority file."

    def test_load_priority_scores_valid_with_spaces_and_tabs(self, tmpdir, argparser, filter_obj:FilterSQLite):
        """Load a priority score file with spaces in strain names."""
        content = "strain 1\t5\nstrain 2\t6\nstrain 3\t8\n"
        priorities_fn = write_file(tmpdir, "priorities.txt", content)
        # --metadata is required but we don't need it
        args = argparser(f'--metadata "" --priority {priorities_fn}')
        filter_obj.set_args(args)
        filter_obj.db_load_priorities_table()
        filter_obj.cur.execute(f"SELECT * FROM {PRIORITIES_TABLE_NAME}")
        table = filter_obj.cur.fetchall()
        assert table == [(0, 'strain 1', 5.0), (1, 'strain 2', 6.0), (2, 'strain 3', 8.0)]

    def test_load_priority_scores_does_not_exist(self, tmpdir, argparser, filter_obj:FilterSQLite):
        """Attempt to load a non-existant priority score file raises a FileNotFoundError."""
        invalid_priorities_fn = str(tmpdir / "does/not/exist.txt")
        args = argparser(f'--metadata "" --priority {invalid_priorities_fn}')
        filter_obj.set_args(args)
        with pytest.raises(FileNotFoundError):
            filter_obj.db_load_priorities_table()

    def test_filter_by_query(self, tmpdir, argparser):
        """Filter by a query expresssion."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir, argparser)
        args.query = 'quality=="good"'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL} FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_query'
        """)
        assert results == [('SEQ_2',)]

    def test_filter_by_query_two_conditions(self, tmpdir, argparser):
        """Filter by a query expresssion with two conditions."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir, argparser)
        args.query = 'quality=="good" AND location=="colorado"'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL} FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_query'
        """)
        assert results == [('SEQ_2',), ('SEQ_3',)]

    def test_filter_by_query_and_include_strains(self, tmpdir, argparser):
        """Filter by a query expresssion and force-include a strain."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        include_fn = str(tmpdir / "include.txt")
        open(include_fn, "w").write("SEQ_3")
        args = get_valid_args(data, tmpdir, argparser)
        args.query = 'quality=="good" AND location=="colorado"'
        args.include = [include_fn]
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        assert results == [('SEQ_1',), ('SEQ_3',)]

    def test_filter_by_query_and_include_where(self, tmpdir, argparser):
        """Filter by a query expresssion and force-include a strain."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        include_fn = str(tmpdir / "include.txt")
        open(include_fn, "w").write("SEQ_3")
        args = get_valid_args(data, tmpdir, argparser)
        args.query = 'quality=="good" AND location=="colorado"'
        args.include_where = ['location=nevada']
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        assert results == [('SEQ_1',), ('SEQ_3',)]

    def test_filter_by_min_date(self, tmpdir, argparser):
        """Filter by min date, inclusive."""
        data = [("strain","date"),
                ("SEQ_1","2020-02-XX"),
                ("SEQ_2","2020-02-26"),
                ("SEQ_3","2020-02-25")]
        args = get_valid_args(data, tmpdir, argparser)
        args.min_date = '2020-02-26'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_min_date'
        """)
        assert results == [('SEQ_3',)]

    def test_filter_by_max_date(self, tmpdir, argparser):
        """Filter by max date, inclusive."""
        data = [("strain","date"),
                ("SEQ_1","2020-03-XX"),
                ("SEQ_2","2020-03-01"),
                ("SEQ_3","2020-03-02")]
        args = get_valid_args(data, tmpdir, argparser)
        args.max_date = '2020-03-01'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_max_date'
        """)
        assert results == [('SEQ_3',)]
