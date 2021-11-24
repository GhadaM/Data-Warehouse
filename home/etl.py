import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies data from S3 Bucket to staging tables located in Redshift cluster 
    """
    count = 0
    for query in copy_table_queries:
        count += 1
        print('======= LOADING TABLE: {} / 2  ======='.format(count))
        cur.execute(query)
        conn.commit()
    print('Done.')


def insert_tables(cur, conn):
    """
    Loads the fact table and dimension tables from the created staging tables
    """
    count = 0
    for query in insert_table_queries:
        count += 1
        print('======= INSERTING TABLE: {} / 5  ======='.format(count))
        cur.execute(query)
        conn.commit()
    print('Done.')


def main():
    """
    The function extracts the configurations needed to connect to the database from dwh.cfg
    after connection to the database is established 
    the function calls load_staging_tables function, 
    then calls the function insert_tables,
    then closes the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DB_NAME = config.get("CLUSTER","DB_NAME")
    DB_USER = config.get("CLUSTER","DB_USER")
    DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT = config.get("CLUSTER","DB_PORT")
    HOST = config.get("CLUSTER","HOST")
    
    conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(DB_NAME, HOST, DB_PORT, DB_USER, DB_PASSWORD))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()