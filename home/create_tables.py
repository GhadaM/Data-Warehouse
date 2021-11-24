import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drop_tables() drops all the tables by looping a list of queries 
    then it closes the connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Table were dropped successfully!")


def create_tables(cur, conn):
    """
    create_tables() loops through list of queries that will create 2 staging tables, 4 dimension tables and 1 fact table
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Table were created successfully!")


def main():
    """
    The function extracts the configurations needed to connect to the database from dwh.cfg
    then it calls the functions drop_tables and create_tables with connection and cursor passed as arguments
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()