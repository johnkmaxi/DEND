import psycopg2
from sql_queries import tables, columns, region_insert
from helper import region_codes

def create_database():
    # connect to default database
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS capstone")
    cur.execute("CREATE DATABASE capstone WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn): #, drop_table_queries
    drop_query = """DROP TABLE IF EXISTS {};"""
    for table in tables:
        print(drop_query.format(table))
        cur.execute(drop_query.format(table))
        conn.commit()


def create_tables(cur, conn): #, create_table_queries, columns
    create_query = """CREATE TABLE IF NOT EXISTS {} {};"""
    for table in tables:
        print(create_query.format(table, columns[table]))
        cur.execute(create_query.format(table, columns[table]))
        conn.commit()
    for k, v in region_codes.items():
        cur.execute(region_insert, [v,k])
        conn.commit()

def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
