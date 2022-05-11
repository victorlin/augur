from augur.io import print_err


def pandas_to_sqlite3(query:str):
    new_query = (query
        .replace('&', 'AND')
        .replace('|', 'OR')
    )
    if query != new_query:
        print_err('WARNING: Pandas query syntax is no longer supported. Please switch to sqlite3 query syntax.')
    return new_query
