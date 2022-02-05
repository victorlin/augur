import argparse
import shlex
import pytest

import augur.filter
from augur.filter_support.db.sqlite import (
    METADATA_TABLE_NAME,
    PRIORITIES_TABLE_NAME,
    FilterSQLite
)

from test_filter import argparser, write_file, write_metadata


@pytest.fixture(scope='function')
def filter_obj():
    """Returns a filter object connected to an in-memory database per function."""
    obj = FilterSQLite(':memory:')
    obj.db_connect()
    return obj


class TestDataLoading:
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
