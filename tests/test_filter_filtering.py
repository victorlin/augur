import argparse

from augur.filter_support.db.sqlite import (
    EXCLUDE_COL,
    FILTER_REASON_COL,
    INCLUDE_COL,
    METADATA_FILTER_REASON_TABLE_NAME,
    STRAIN_COL,
    FilterSQLite
)

from test_filter import parse_args, write_metadata


def get_filter_obj_run(args:argparse.Namespace):
    """Returns a filter object connected to an in-memory database with run() invoked."""
    obj = FilterSQLite(':memory:')
    obj.set_args(args)
    obj.run(cleanup=False)
    return obj


def get_valid_args(data, tmpdir):
    """Returns an argparse.Namespace with metadata and output_strains"""
    meta_fn = write_metadata(tmpdir, data)
    return parse_args(f'--metadata {meta_fn} --output-strains {tmpdir / "strains.txt"}')


def query_fetchall(filter_obj:FilterSQLite, query:str):
    filter_obj.cur.execute(query)
    return filter_obj.cur.fetchall()


class TestFiltering:
    def test_filter_by_query(self, tmpdir):
        """Filter by a query expresssion."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        args.query = 'quality=="good"'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL} FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_query'
        """)
        assert results == [('SEQ_2',)]

    def test_filter_by_query_two_conditions(self, tmpdir):
        """Filter by a query expresssion with two conditions."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        args.query = 'quality=="good" AND location=="colorado"'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL} FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_query'
        """)
        assert results == [('SEQ_2',), ('SEQ_3',)]

    def test_filter_by_query_and_include_strains(self, tmpdir):
        """Filter by a query expresssion and force-include a strain."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        include_fn = str(tmpdir / "include.txt")
        open(include_fn, "w").write("SEQ_3")
        args = get_valid_args(data, tmpdir)
        args.query = 'quality=="good" AND location=="colorado"'
        args.include = [include_fn]
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        assert results == [('SEQ_1',), ('SEQ_3',)]

    def test_filter_by_query_and_include_where(self, tmpdir):
        """Filter by a query expresssion and force-include a strain."""
        data = [("strain","location","quality"),
                ("SEQ_1","colorado","good"),
                ("SEQ_2","colorado","bad"),
                ("SEQ_3","nevada","good")]
        args = get_valid_args(data, tmpdir)
        args.query = 'quality=="good" AND location=="colorado"'
        args.include_where = ['location=nevada']
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE NOT {EXCLUDE_COL} OR {INCLUDE_COL}
        """)
        assert results == [('SEQ_1',), ('SEQ_3',)]

    def test_filter_by_min_date(self, tmpdir):
        """Filter by min date, inclusive."""
        data = [("strain","date"),
                ("SEQ_1","2020-02-XX"),
                ("SEQ_2","2020-02-26"),
                ("SEQ_3","2020-02-25")]
        args = get_valid_args(data, tmpdir)
        args.min_date = '2020-02-26'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_min_date'
        """)
        assert results == [('SEQ_3',)]

    def test_filter_by_max_date(self, tmpdir):
        """Filter by max date, inclusive."""
        data = [("strain","date"),
                ("SEQ_1","2020-03-XX"),
                ("SEQ_2","2020-03-01"),
                ("SEQ_3","2020-03-02")]
        args = get_valid_args(data, tmpdir)
        args.max_date = '2020-03-01'
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {STRAIN_COL}
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = 'filter_by_max_date'
        """)
        assert results == [('SEQ_3',)]
