import argparse
import shlex
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


@pytest.fixture
def tmp_db_file(tmpdir):
    return str(tmpdir / "test.sqlite3")


def get_init_filter_obj(args:argparse.Namespace, tmp_db_file):
    filter_obj = FilterSQLite(args)
    filter_obj.db_connect(tmp_db_file)
    return filter_obj


class TestFilter:
    def test_load_metadata(self, tmpdir, argparser, tmp_db_file):
        """Load a metadata file."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        meta_fn = write_metadata(tmpdir, data)
        args = argparser(f'--metadata {meta_fn}')
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        filter_obj.db_load_metadata(tmp_db_file)
        filter_obj.cur.execute(f"SELECT * FROM {METADATA_TABLE_NAME}")
        table = filter_obj.cur.fetchall()
        filter_obj.connection.close()
        filter_obj.db_cleanup()
        assert [row[1:] for row in table] == data[1:]

    def test_load_priority_scores_valid(self, tmpdir, argparser, tmp_db_file):
        """Load a priority score file."""
        content = "strain1\t5\nstrain2\t6\nstrain3\t8\n"
        priorities_fn = write_file(tmpdir, "priorities.txt", content)
        # --metadata is required but we don't need it
        args = argparser(f'--metadata "" --priority {priorities_fn}')
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        filter_obj.db_load_priorities_table(tmp_db_file)
        filter_obj.cur.execute(f"SELECT * FROM {PRIORITIES_TABLE_NAME}")
        table = filter_obj.cur.fetchall()
        filter_obj.connection.close()
        filter_obj.db_cleanup()
        assert table == [(0, 'strain1', 5.0), (1, 'strain2', 6.0), (2, 'strain3', 8.0)]

    def test_load_priority_scores_malformed(self, tmpdir, argparser, tmp_db_file):
        """Attempt to load a priority score file with non-float in priority column raises a ValueError."""
        content = "strain1 X\n"
        priorities_fn = write_file(tmpdir, "priorities.txt", content)
        # --metadata is required but we don't need it
        args = argparser(f'--metadata "" --priority {priorities_fn}')
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        with pytest.raises(ValueError) as e_info:
            filter_obj.db_load_priorities_table(tmp_db_file)
        filter_obj.connection.close()
        filter_obj.db_cleanup()
        assert str(e_info.value) == "Failed to parse priority file."

    def test_load_priority_scores_valid_with_spaces_and_tabs(self, tmpdir, argparser, tmp_db_file):
        """Load a priority score file with spaces in strain names."""
        content = "strain 1\t5\nstrain 2\t6\nstrain 3\t8\n"
        priorities_fn = write_file(tmpdir, "priorities.txt", content)
        # --metadata is required but we don't need it
        args = argparser(f'--metadata "" --priority {priorities_fn}')
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        filter_obj.db_load_priorities_table(tmp_db_file)
        filter_obj.cur.execute(f"SELECT * FROM {PRIORITIES_TABLE_NAME}")
        table = filter_obj.cur.fetchall()
        filter_obj.connection.close()
        filter_obj.db_cleanup()
        assert table == [(0, 'strain 1', 5.0), (1, 'strain 2', 6.0), (2, 'strain 3', 8.0)]

    def test_load_priority_scores_does_not_exist(self, tmpdir, argparser, tmp_db_file):
        """Attempt to load a non-existant priority score file raises a FileNotFoundError."""
        invalid_priorities_fn = str(tmpdir / "does/not/exist.txt")
        args = argparser(f'--metadata "" --priority {invalid_priorities_fn}')
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        with pytest.raises(FileNotFoundError):
            filter_obj.db_load_priorities_table(tmp_db_file)
        filter_obj.connection.close()
        filter_obj.db_cleanup()

    def test_filter_by_query(self, tmpdir, argparser, tmp_db_file):
        """Filter by a query expresssion."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        meta_fn = write_metadata(tmpdir, data)
        args = argparser(f'--metadata {meta_fn} --query \'quality=="good"\'')
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        filter_obj.db_load_metadata(tmp_db_file)
        filter_obj.add_attributes()
        exclude_by, include_by = filter_obj.construct_filters()
        filter_obj.db_create_filter_reason_table(exclude_by, include_by)
        filter_obj.cur.execute(f"""
            SELECT {STRAIN_COL} FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_query'
        """)
        results = filter_obj.cur.fetchall()
        filter_obj.connection.close()
        filter_obj.db_cleanup()
        assert results == [('SEQ_2',)]

    def test_filter_by_query_two_conditions(self, tmpdir, argparser, tmp_db_file):
        """Filter by a query expresssion with two conditions."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        meta_fn = write_metadata(tmpdir, data)
        args = argparser(f"""
            --metadata {meta_fn} --query 'quality=="good" AND location=="colorado"'
        """)
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        filter_obj.db_load_metadata(tmp_db_file)
        filter_obj.add_attributes()
        exclude_by, include_by = filter_obj.construct_filters()
        filter_obj.db_create_filter_reason_table(exclude_by, include_by)
        filter_obj.cur.execute(f"""
            SELECT {STRAIN_COL} FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_query'
        """)
        results = filter_obj.cur.fetchall()
        filter_obj.connection.close()
        filter_obj.db_cleanup()
        assert results == [('SEQ_2',), ('SEQ_3',)]

    def test_filter_by_query_and_include_strains(self, tmpdir, argparser, tmp_db_file):
        """Filter by a query expresssion and force-include a strain."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        meta_fn = write_metadata(tmpdir, data)
        include_fn = str(tmpdir / "include.txt")
        open(include_fn, "w").write("SEQ_3")
        args = argparser(f"""
            --metadata {meta_fn}
            --query 'quality=="good" AND location=="colorado"'
            --include {include_fn}
        """)
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        filter_obj.db_load_metadata(tmp_db_file)
        filter_obj.add_attributes()
        exclude_by, include_by = filter_obj.construct_filters()
        filter_obj.db_create_filter_reason_table(exclude_by, include_by)
        filter_obj.cur.execute(f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        results = filter_obj.cur.fetchall()
        filter_obj.connection.close()
        filter_obj.db_cleanup()
        assert results == [('SEQ_1',), ('SEQ_3',)]

    def test_filter_by_query_and_include_where(self, tmpdir, argparser, tmp_db_file):
        """Filter by a query expresssion and force-include a strain."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        meta_fn = write_metadata(tmpdir, data)
        args = argparser(f"""
            --metadata {meta_fn}
            --query 'quality=="good" AND location=="colorado"'
            --include-where 'location=nevada'
        """)
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        filter_obj.db_load_metadata(tmp_db_file)
        filter_obj.add_attributes()
        exclude_by, include_by = filter_obj.construct_filters()
        filter_obj.db_create_filter_reason_table(exclude_by, include_by)
        filter_obj.cur.execute(f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        results = filter_obj.cur.fetchall()
        filter_obj.connection.close()
        filter_obj.db_cleanup()
        assert results == [('SEQ_1',), ('SEQ_3',)]

    def test_filter_by_min_date(self, tmpdir, argparser, tmp_db_file):
        """Filter by min date, inclusive."""
        data = [("strain","date"),
                ("SEQ_1","2020-02-XX"),
                ("SEQ_2","2020-02-26"),
                ("SEQ_3","2020-02-25")]
        meta_fn = write_metadata(tmpdir, data)
        args = argparser(f"""
            --metadata {meta_fn}
            --min-date 2020-02-26
        """)
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        filter_obj.db_load_metadata(tmp_db_file)
        filter_obj.add_attributes()
        filter_obj.db_create_date_table()
        exclude_by, include_by = filter_obj.construct_filters()
        filter_obj.db_create_filter_reason_table(exclude_by, include_by)
        filter_obj.cur.execute(f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_min_date'
        """)
        results = filter_obj.cur.fetchall()
        filter_obj.connection.close()
        filter_obj.db_cleanup()
        assert results == [('SEQ_3',)]

    def test_filter_by_max_date(self, tmpdir, argparser, tmp_db_file):
        """Filter by max date, inclusive."""
        data = [("strain","date"),
                ("SEQ_1","2020-03-XX"),
                ("SEQ_2","2020-03-01"),
                ("SEQ_3","2020-03-02")]
        meta_fn = write_metadata(tmpdir, data)
        args = argparser(f"""
            --metadata {meta_fn}
            --max-date 2020-03-01
        """)
        filter_obj = get_init_filter_obj(args, tmp_db_file)
        filter_obj.db_load_metadata(tmp_db_file)
        filter_obj.add_attributes()
        filter_obj.db_create_date_table()
        exclude_by, include_by = filter_obj.construct_filters()
        filter_obj.db_create_filter_reason_table(exclude_by, include_by)
        filter_obj.cur.execute(f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_max_date'
        """)
        results = filter_obj.cur.fetchall()
        filter_obj.connection.close()
        filter_obj.db_cleanup()
        assert results == [('SEQ_3',)]