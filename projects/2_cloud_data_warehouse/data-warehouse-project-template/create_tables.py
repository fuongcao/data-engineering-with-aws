import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print("executing query \"{}\"".format(query))
        cur.execute(drop_table_queries[query])
        conn.commit()
        print("query \"{}\" was commited".format(query))


def create_tables(cur, conn):
    for query in create_table_queries:
        print("executing query \"{}\"".format(query))
        cur.execute(create_table_queries[query])
        conn.commit()
        print("query \"{}\" was commited".format(query))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()