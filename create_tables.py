import configparser
import psycopg2
from sql_setup_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop any existing tables
    
    Arguments:
    cur -- cursor to connected DB (Allows to execute SQL commands)
    conn -- connection to Postgres database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create new tables
    
    Arguments:
    cur -- cursor to connected DB (Allows to execute SQL commands)
    conn -- connection to Postgres database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to AWS Redshift, create DB, Drop existing tables and create new tables. Close DB connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh_aws.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()