import configparser
import argparse
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def print_verbose(tag, query):
    """
    Prints current query if "--verbose" or "-v" is used.

    Parameters:
    - tag: the action or goal of the current query
    - query: the executing SQL query

    Returns:
        None
    """
    print(tag)
    print("-----")
    print(query, "\n")


def drop_tables(cur, conn, verbose):
    """
    Erase all tables (if exist) in the Redshift cluster.
    
    Parameters:
    - cur: cursor of connection to database
    - conn: connection to Redshift cluster
    - verbose: the boolean determines if queries are printed out

    Returns:
        None
    """

    for tag, query in drop_table_queries.items():
        if verbose:
            print_verbose(tag, query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, verbose):
    """
    Create staging, dimension and fact tables in the Redshift cluster.
    
    Parameters:
    - cur: cursor of connection to database
    - conn: connection to Redshift cluster
    - verbose: the boolean determines if queries are printed out

    Returns:
        None
    """
    for tag, query in create_table_queries.items():
        if verbose:
            print_verbose(tag, query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main process.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="print executing query")
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn, args.verbose)
    create_tables(cur, conn, args.verbose)

    conn.close()


if __name__ == "__main__":
    main()