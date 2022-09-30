from augur.filter.engines.sqlite.sqlite import FilterSQLite


def run(args):
    filter = FilterSQLite(args)
    filter.try_run()
