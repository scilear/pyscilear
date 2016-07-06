import os
from logbook import info, debug
import sqlite3
from sqlalchemy import create_engine

def get_sqlite_connection():
    financial_db = os.environ['FINANCIAL_DB']
    conn = sqlite3.connect(financial_db)
    return conn

def get_sqalchemy_engine():
    financial_db = os.environ['FINANCIAL_DB']
    return create_engine("sqlite:///%s" % financial_db)

def execute_query(sql_query):
    conn = get_sqlite_connection()
    cur = conn.cursor()
    debug(sql_query)
    cur.execute(sql_query)
    conn.commit()

def execute_scalar(sql_query, data=None):
    conn = get_sqlite_connection()
    cur = conn.cursor()
    debug(sql_query)
    if data is None:
        cur.execute(sql_query)
    else:
        cur.execute(sql_query % data)
    results = cur.fetchone()
    return results

def execute_cursor(sql_query, data=None):
    conn = get_sqlite_connection()
    cur = conn.cursor()
    debug(sql_query)
    if data is None:
        cur.execute(sql_query)
    else:
        cur.execute(sql_query % data)
    results = cur.fetchall()
    return results

def create_financial_db():
    pass

