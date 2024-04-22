import configparser
import psycopg2
import time
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        start_time = time.time()
        print("executing query \"{}\"".format(query))
        cur.execute(copy_table_queries[query])
        conn.commit()
        print("query \"{}\" was commited - excution time {}".format(query, (time.time() - start_time)))


def insert_tables(cur, conn):
    for query in insert_table_queries:
        start_time = time.time()
        print("executing query \"{}\"".format(query))
        cur.execute(insert_table_queries[query])
        conn.commit()
        print("query \"{}\" was commited - excution time {}".format(query, (time.time() - start_time)))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()