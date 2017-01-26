import os
import pandas as pd
import psycopg2
from logbook import info, trace, error
from sqlalchemy import create_engine
from upsert import Upsert

PG_CONNECTION = None

def get_dataframe(sql, index_col=None, coerce_float=True, params=None,
                  parse_dates=None):
    return pd.read_sql(sql, con=get_sqalchemy_engine(), index_col=index_col, coerce_float=coerce_float, params=params,
                       parse_dates=parse_dates)


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
        trace(ak)
    else:  # TODO redundant with pg defaults...
        db_name = os.environ['PGDATABASE']
        db_user = os.environ['PGUSER']
        db_password = os.environ['PGPASSWORD']
        db_host = os.environ['PGHOST']
    return db_name, db_user, db_password, db_host


def log_error(function_name, what, exception_str):
    error('%s - error on %s - %s' % (function_name, what, exception_str))
    rollback(True)



def rollback(reset=False):
    if PG_CONNECTION is not None:
        PG_CONNECTION.rollback()
        global UPSERTERS
        UPSERTERS = dict()
        if reset:
            try:
                PG_CONNECTION.close()
            except Exception as e:
                error('%s - error on connection close - %s' % (__name__, str(e)))
            finally:
                PG_CONNECTION = None


def commit():
    if PG_CONNECTION is not None and PG_CONNECTION.closed != 0:
        PG_CONNECTION.commit()


def get_pg_connection():
    global PG_CONNECTION
    if PG_CONNECTION is not None and PG_CONNECTION.closed == 0:
        return PG_CONNECTION

    db, user, pwd, host = get_db_access()
    conn_string = "dbname='%s' port='5432' user='%s' password='%s' host='%s'" % (db, user, pwd, host);
    trace(conn_string)
    PG_CONNECTION = psycopg2.connect(conn_string)
    return PG_CONNECTION


PG_CONNECTION = get_pg_connection()


def execute_query(sql_query, data=None, autocommit=True):
    try:
        conn = get_pg_connection()
        if autocommit:
            old_isolation_level = conn.isolation_level
            conn.set_isolation_level(0)
        cur = conn.cursor()
        trace(sql_query)
        cur.execute(sql_query, data)
        if autocommit:
            conn.set_isolation_level(old_isolation_level)
        conn.commit()
    except Exception as e:
        log_error(__name__, sql_query, str(e))


def execute_scalar(sql_query, data=None):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        trace(sql_query)
        cur.execute(sql_query, data)
        conn.commit()  # needed when we return the id of an insert for instance
        results = cur.fetchone()
        return results
    except Exception as e:
        log_error(__name__, sql_query, str(e))


def execute_cursor(sql_query, data=None):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        trace('%s --params-- %s' % (sql_query, data))
        cur.execute(sql_query, data)
        conn.commit()  # needed when we return the id of an insert for instance
        results = cur.fetchall()
        return results
    except Exception as e:
        log_error(__name__, sql_query, str(e))


def execute_cursor_function(function_name, data=None):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.callproc(function_name, data)
        conn.commit()
        rows = cur.fetchall()
        return rows
    except Exception as e:
        log_error(__name__, function_name, str(e))


UPSERTERS = dict()


def upsert(table, reset=False):
    global UPSERTERS
    try:
        if reset or not table in UPSERTERS:
            conn = get_pg_connection()
            cur = conn.cursor()
            upsert = Upsert(cur, table)
            UPSERTERS[table] = upsert
        else:
            upsert = UPSERTERS[table]
            if upsert.cursor.closed != 0 or upsert.cursor.connection.closed != 0 or upsert is None:
                return upsert(table, True)
        assert (isinstance(upsert, Upsert))
        return upsert
    except Exception as e:
        log_error(__name__, table, str(e))
        return None


def query_to_file(query, file_name):
    conn = get_pg_connection()
    cur = conn.cursor()
    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
    with open(file_name, 'w') as f:
        cur.copy_expert(outputquery, f)
    conn.commit()


def file_to_table(table, file_name, columns=None):
    conn = get_pg_connection()
    cur = conn.cursor()
    if columns is None:
        outputquery = "COPY %s FROM stdin WITH CSV HEADER DELIMITER as ',' " % table
    else:
        outputquery = "COPY %s(day,%s) FROM stdin WITH CSV HEADER DELIMITER as ',' " % (table, ','.join(columns))
    with open(file_name, 'r') as f:
        cur.copy_expert(outputquery, f)

    f.close()
    conn.commit()


if __name__ == "__main__":
    # res = execute_cursor('select * from daily_data limit 10')
    # print res

    # large_query = "select * from daily_ema_dataset limit 1000000"
    # query_to_file(large_query, 'large_query.csv')

    filename = 'test_queries.csv'
    query_to_file('select * from daily_data limit 100', filename)
    execute_query('create table test_pyscilear as select * from daily_data where 1=2')
    file_to_table('test_pyscilear', filename)
    print(execute_scalar('select count(*) from test_pyscilear')[0])
    execute_query('drop table test_pyscilear')

    """
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.callproc("get_pending_jobs", ('news_sent', 10))

    # named_cursor = conn.cursor(name='cursor_x')
    for row in cur:
        print row
    """
