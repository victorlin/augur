import pytest

from augur.filter_support.db.sqlite import (
    METADATA_TABLE_NAME,
    PRIORITIES_TABLE_NAME,
)

from test_filter import write_file
from tests.test_filter import get_filter_obj_run, get_valid_args, query_fetchall


def get_filter_obj_with_priority_loaded(tmpdir, content:str):
    priorities_fn = write_file(tmpdir, "priorities.txt", content)
    # metadata is a required arg but we don't need it
    data = [("strain","location","quality"),
            ("SEQ_1","colorado","good")]
    args = get_valid_args(data, tmpdir)
    args.priority = priorities_fn
    return get_filter_obj_run(args)


class TestDataLoading:
    def test_load_metadata(self, tmpdir):
        """Load a metadata file."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"SELECT * FROM {METADATA_TABLE_NAME}")
        assert [row[1:] for row in results] == data[1:]

    def test_load_priority_scores_valid(self, tmpdir):
        """Load a priority score file."""
        content = "strain1\t5\nstrain2\t6\nstrain3\t8\n"
        filter_obj = get_filter_obj_with_priority_loaded(tmpdir, content)
        filter_obj.db_load_priorities_table()
        results = query_fetchall(filter_obj, f"SELECT * FROM {PRIORITIES_TABLE_NAME}")
        assert results == [(0, 'strain1', 5.0), (1, 'strain2', 6.0), (2, 'strain3', 8.0)]

    def test_load_priority_scores_malformed(self, tmpdir):
        """Attempt to load a priority score file with non-float in priority column raises a ValueError."""
        content = "strain1 X\n"
        filter_obj = get_filter_obj_with_priority_loaded(tmpdir, content)
        with pytest.raises(ValueError) as e_info:
            filter_obj.db_load_priorities_table()
        assert str(e_info.value) == "Failed to parse priority file."

    def test_load_priority_scores_valid_with_spaces_and_tabs(self, tmpdir):
        """Load a priority score file with spaces in strain names."""
        content = "strain 1\t5\nstrain 2\t6\nstrain 3\t8\n"
        filter_obj = get_filter_obj_with_priority_loaded(tmpdir, content)
        filter_obj.db_load_priorities_table()
        results = query_fetchall(filter_obj, f"SELECT * FROM {PRIORITIES_TABLE_NAME}")
        assert results == [(0, 'strain 1', 5.0), (1, 'strain 2', 6.0), (2, 'strain 3', 8.0)]

    def test_load_priority_scores_does_not_exist(self, tmpdir):
        """Attempt to load a non-existant priority score file raises a FileNotFoundError."""
        invalid_priorities_fn = str(tmpdir / "does/not/exist.txt")
        # metadata is a required arg but we don't need it
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good")]
        args = get_valid_args(data, tmpdir)
        args.priority = invalid_priorities_fn
        filter_obj = get_filter_obj_run(args)
        with pytest.raises(FileNotFoundError):
            filter_obj.db_load_priorities_table()
