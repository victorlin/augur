import argparse
import shlex
import pytest
import sqlite3
import augur.filter

from augur.io_support.db.sqlite import (
    DEFAULT_DB_FILE
)
from augur.filter_support.db.sqlite import (
    METADATA_TABLE_NAME,
    PRIORITIES_TABLE_NAME,
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


@pytest.fixture
def tmp_db_file(tmpdir):
    return str(tmpdir / "test.sqlite3")


def db_query_fetchall(query:str):
    con = sqlite3.connect(DEFAULT_DB_FILE)
    cur = con.cursor()
    cur.execute(query)
    return cur.fetchall()


class TestFilter:
    def test_load_metadata(self, tmpdir, argparser):
        """Load a metadata file."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        meta_fn = write_metadata(tmpdir, data)
        args = argparser(f'--metadata {meta_fn}')
        filter_obj = FilterSQLite(args)
        filter_obj.db_cleanup()
        filter_obj.db_connect() # TODO: parameterize db file
        filter_obj.db_load_metadata()
        table = db_query_fetchall(f"SELECT * FROM {METADATA_TABLE_NAME}")
        assert [row[1:] for row in table] == data[1:]

    def test_load_priority_scores_valid(self, tmpdir, argparser):
        """Load a priority score file."""
        content = "strain1\t5\nstrain2\t6\nstrain3\t8\n"
        priorities_fn = write_file(tmpdir, "priorities.txt", content)
        # --metadata is required but we don't need it
        args = argparser(f'--metadata "" --priority {priorities_fn}')
        filter_obj = FilterSQLite(args)
        filter_obj.db_cleanup()
        filter_obj.db_connect()
        filter_obj.db_load_priorities_table()
        table = db_query_fetchall(f"SELECT * FROM {PRIORITIES_TABLE_NAME}")
        assert table == [(0, 'strain1', 5.0), (1, 'strain2', 6.0), (2, 'strain3', 8.0)]

    def test_load_priority_scores_malformed(self, tmpdir, argparser):
        """Attempt to load a priority score file with non-float in priority column raises a ValueError."""
        content = "strain1 X\n"
        priorities_fn = write_file(tmpdir, "priorities.txt", content)
        # --metadata is required but we don't need it
        args = argparser(f'--metadata "" --priority {priorities_fn}')
        filter_obj = FilterSQLite(args)
        filter_obj.db_cleanup()
        filter_obj.db_connect()
        with pytest.raises(ValueError) as e_info:
            filter_obj.db_load_priorities_table()
        assert str(e_info.value) == "Failed to parse priority file."

    def test_load_priority_scores_valid_with_spaces_and_tabs(self, tmpdir, argparser):
        """Load a priority score file with spaces in strain names."""
        content = "strain 1\t5\nstrain 2\t6\nstrain 3\t8\n"
        priorities_fn = write_file(tmpdir, "priorities.txt", content)
        # --metadata is required but we don't need it
        args = argparser(f'--metadata "" --priority {priorities_fn}')
        filter_obj = FilterSQLite(args)
        filter_obj.db_cleanup()
        filter_obj.db_connect()
        filter_obj.db_load_priorities_table()
        table = db_query_fetchall(f"SELECT * FROM {PRIORITIES_TABLE_NAME}")
        assert table == [(0, 'strain 1', 5.0), (1, 'strain 2', 6.0), (2, 'strain 3', 8.0)]

    def test_load_priority_scores_does_not_exist(self, tmpdir, argparser):
        """Attempt to load a non-existant priority score file raises a FileNotFoundError."""
        invalid_priorities_fn = str(tmpdir / "does/not/exist.txt")
        args = argparser(f'--metadata "" --priority {invalid_priorities_fn}')
        filter_obj = FilterSQLite(args)
        filter_obj.db_cleanup()
        filter_obj.db_connect()
        with pytest.raises(FileNotFoundError):
            filter_obj.db_load_priorities_table()
