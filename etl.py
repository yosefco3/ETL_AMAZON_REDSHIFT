import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,drop_without_staging, create_without_staging, drop_table_queries, create_table_queries


# creating the staging tables takes time. if they already created and 
# we don't want to create them again, 
# just to check the "insert" statements for example, change this to false:
CREATE_STAGING_TABLES = True


def drop_tables(cur, conn):
    """
    connect to the database and drop all tables if exists
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
  
def drop_without(cur, conn):
    """
    connect to the database and drop all tables if exists without the staging tables
    """
    for query in drop_without_staging:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    creates all the tables
    """
    for query in create_table_queries:
        cur.execute(query)
        print(f' executed {query}')
        conn.commit()
    
def create_without(cur, conn):
    """
    creates all the tables except the staging tables
    """
    for query in create_without_staging:
        cur.execute(query)
        conn.commit()
        
def load_staging_tables(cur, conn):
    """
    copy all the data from s3 to the staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        print(f' executed {query}')
        conn.commit()


def insert_tables(cur, conn):
    """
    inserts all the data from the staging tables to the final tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        print(f' executed {query}')
        conn.commit()


        
def main_without_staging():
    '''
        the main function for the case the staging tables already exist:
        (mainly for debuging purposes, we dont want to create again and again the staging - it takes time)
        read parameters from the configuration file, 
        connects to the redshift cluster, 
        creates tables, 
        copy data from the staging tables to the final table.
    
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    drop_without(cur, conn)
    print('dropped******')
    create_without(cur, conn)
    print('created******')
    insert_tables(cur, conn)
    print('************inserted*********************')
    conn.close()
    
def main():
    '''
        the main function:
        read parameters from the configuration file, 
        connects to the redshift cluster, 
        creates tables, 
        copy data from s3 to the staging tables,
        copy data from the staging tables to the final table.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    drop_tables(cur, conn)
    print('dropped')
    create_tables(cur, conn)
    print('************tables created*********************')
    load_staging_tables(cur, conn)
    print('************stage tables loaded****************')
    insert_tables(cur, conn)
    print('************inserted*********************')
    conn.close()


if __name__ == "__main__":
    if CREATE_STAGING_TABLES:
        main()
    else:
        main_without_staging()