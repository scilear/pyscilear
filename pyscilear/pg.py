import os

import pandas as pd
import psycopg2
from logbook import info, debug, error
from sqlalchemy import create_engine
from upsert import Upsert


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


PG_CONNECTION = None


def log_error(function_name, what, exception_str):
    error('%s - error on %s - %s' % (function_name, what, exception_str))
    rollback(True)


def rollback(reset=False):
    global PG_CONNECTION
    if PG_CONNECTION is not None:
        PG_CONNECTION.rollback()
        if reset:
            try:
                PG_CONNECTION.close()
            except Exception, e:
                error('%s - error on connection close - %s' % (__name__, str(e)))
            finally:
                PG_CONNECTION = None


def commit():
    global PG_CONNECTION
    if PG_CONNECTION is not None:
        PG_CONNECTION.commit()


def get_pg_connection():
    global PG_CONNECTION
    if PG_CONNECTION is not None:
        return PG_CONNECTION

    db, user, pwd, host = get_db_access()
    conn_string = "dbname='%s' port='5432' user='%s' password='%s' host='%s'" % (db, user, pwd, host);
    debug(conn_string)
    PG_CONNECTION = psycopg2.connect(conn_string)
    return PG_CONNECTION


def execute_query(sql_query, data=None):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        debug(sql_query)
        cur.execute(sql_query, data)
        conn.commit()
    except Exception, e:
        log_error(__name__, sql_query, str(e))



def execute_scalar(sql_query, data=None):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        debug(sql_query)
        cur.execute(sql_query, data)
        conn.commit()  # needed when we return the id of an insert for instance
        results = cur.fetchone()
        return results
    except Exception, e:
        log_error(__name__, sql_query, str(e))



def execute_cursor(sql_query, data=None):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        debug(sql_query)
        cur.execute(sql_query, data)
        conn.commit()  # needed when we return the id of an insert for instance
        results = cur.fetchall()
        return results
    except Exception, e:
        log_error(__name__, sql_query, str(e))



def execute_cursor_function(function_name, data=None):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.callproc(function_name, data)
        conn.commit()
        rows = cur.fetchall()
        return rows
    except Exception, e:
        log_error(__name__, function_name, str(e))


def get_upsert(table):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        upsert = Upsert(cur, table)
        return upsert
    except Exception, e:
        log_error(__name__, table, str(e))


if __name__ == "__main__":
    # res = execute_cursor('select * from daily_data limit 10')
    # print res

    conn = get_pg_connection()
    cur = conn.cursor()
    cur.callproc("get_pending_jobs", ('news_sent', 10))

    # named_cursor = conn.cursor(name='cursor_x')
    for row in cur:
        print row
