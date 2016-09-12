import os

import pandas as pd
import psycopg2
from logbook import info, debug
from sqlalchemy import create_engine


def get_sqalchemy_engine():
    db, user, pwd, host = get_db_access()
    return create_engine('postgresql://%s:%s@%s/%s' % (user, pwd, host, db))


def get_db_access():
    if os.path.exists('db_access'):
        ak = pd.read_csv('db_access')
        db_name = ak.values[0][0]
        db_user = ak.values[0][1]
        db_password = ak.values[0][2]
        db_host = ak.values[0][3]
        debug(ak)
    else:
        db_name = os.environ['PG_NAME']
        db_user = os.environ['PG_USER']
        db_password = os.environ['PG_PASSWORD']
        db_host = os.environ['PG_HOST']
    return db_name, db_user, db_password, db_host


def get_pg_connection():
    db, user, pwd, host = get_db_access()
    conn_string = "dbname='%s' port='5432' user='%s' password='%s' host='%s'" % (db, user, pwd, host);
    debug(conn_string)
    conn = psycopg2.connect(conn_string)
    return conn


def execute_query(sql_query):
    conn = get_pg_connection()
    cur = conn.cursor()
    debug(sql_query)
    cur.execute(sql_query)
    conn.commit()


def execute_scalar(sql_query, data=None):
    conn = get_pg_connection()
    cur = conn.cursor()
    debug(sql_query)
    cur.execute(sql_query, data)
    results = cur.fetchone()
    return results


def execute_cursor(sql_query, data=None):
    conn = get_pg_connection()
    cur = conn.cursor()
    debug(sql_query)
    cur.execute(sql_query, data)
    results = cur.fetchall()
    return results


if __name__ == "__main__":
    res = execute_cursor('select * from daily_data limit 10')
    print res
