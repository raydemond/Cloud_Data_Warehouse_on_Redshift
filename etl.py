import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Extracts data from S3 and stage them in Redshift.
    
    Args:
        cur: cursor from the connection
        conn: connection to the database
        
    Returns:
        None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Transforms staging tables into a set of fact table and dimensional tables.
    
    Args:
        cur: cursor from the connection
        conn: connection to the database
        
    Returns:
        None
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    - Reads the configuration file
    
    - Establishes connection with the Redshift database and gets
    cursor to it.  
    
    - Loads data from S3 into staging tables. 
    
    - Transforms staging tables into star schema.
    
    - Finally, closes the connection.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()