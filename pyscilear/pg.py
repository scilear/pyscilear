import os
import pandas as pd
import psycopg2
import psycopg2.extras
from logbook import info, trace, error, debug
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from pyscilear.utils import log

PG_CONNECTION = None


def get_dataframe(sql, index_col=None, coerce_float=True, params=None,
                  parse_dates=None):
    return pd.read_sql(sql, con=get_sqlalchemy_engine(), index_col=index_col, coerce_float=coerce_float, params=params,
                       parse_dates=parse_dates)


def get_sqlalchemy_engine(db_name=None):
    db, user, pwd, host = get_db_access(db_name)
    # 4 options

    # 1-
    # return create_engine('postgresql://%s:%s@%s/%s' % (user, pwd, host, db))
    # 2-
    # return create_engine('postgresql://%s:%s@%s/%s' % (user, pwd, host, db), pool_size=5, max_overflow=0)

    # 3-
    # import socket
    # import sys
    # from pilfile import args

    # prog = os.path.basename(sys.argv[0]) or 'desjob'
    # username = pwd.getpwuid (os.getuid ()).pw_name
    # hostname = socket.gethostname().split(".")[0]·
    # args.setdefault('connect_args', {'application_name': "%s:%s@%s" %
    #     (prog, username, hostname)})
    # args.setdefault('isolation_level', "AUTOCOMMIT")
    # engine = create_engine(url, **args)
    # return engine

    # 4-
    return create_engine('postgresql://%s:%s@%s/%s' % (user, pwd, host, db), poolclass=NullPool)


def get_db_access(db_name=''):
    if db_name is None or db_name == '':
        db_name = os.environ['PGDATABASE']
    db_user = os.environ['PGUSER']
    db_password = os.environ['PGPASSWORD']
    db_host = os.environ['PGHOST']
    return db_name, db_user, db_password, db_host


def log_error(function_name, what, exception_str):
    error('%s - error on %s - %s' % (function_name, what, exception_str))
    rollback(True)


def rollback(reset=False):
    global PG_CONNECTION
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
    global PG_CONNECTION
    if PG_CONNECTION is not None and PG_CONNECTION.closed != 0:
        PG_CONNECTION.commit()


def get_pg_connection(db_name=None):
    global PG_CONNECTION
    if PG_CONNECTION is not None and PG_CONNECTION.closed == 0:
        return PG_CONNECTION

    db, user, pwd, host = get_db_access(db_name=db_name)
    conn_string = "dbname='%s' port='5432' user='%s' password='%s' host='%s'" % (db, user, pwd, host)
    trace(conn_string)
    log('creating new connection', debug)
    PG_CONNECTION = psycopg2.connect(conn_string)
    PG_CONNECTION.cursor().execute('set search_path = news,public;')
    return PG_CONNECTION


PG_CONNECTION = get_pg_connection()


def insert_many(sql_query, data=None, commit_right_after=True, autocommit=True, auto_catch=False):
    try:
        conn = get_pg_connection()
        if autocommit:
            old_isolation_level = conn.isolation_level
            conn.set_isolation_level(0)
        cur = conn.cursor()
        trace(sql_query)
        psycopg2.extras.execute_values(cur, sql_query, data, template=None, page_size=100)
        if autocommit:
            conn.set_isolation_level(old_isolation_level)
        if commit_right_after:
            conn.commit()
    except Exception as e:
        if auto_catch:
            log_error(__name__, sql_query, str(e))
        else:
            raise

def execute_query(sql_query, data=None, commit_right_after=True, autocommit=True, auto_catch=False):
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
        if commit_right_after:
            conn.commit()
    except Exception as e:
        if auto_catch:
            log_error(__name__, sql_query, str(e))
        else:
            raise


def execute_scalar(sql_query, data=None, commit_right_after=True, db_name=None, auto_catch=False):
    try:
        conn = get_pg_connection(db_name)
        cur = get_pg_connection(db_name).cursor()
        trace(sql_query)
        cur.execute(sql_query, data)
        if commit_right_after:
            conn.commit()  # needed when we return the id of an insert for instance
        results = cur.fetchone()
        return results
    except Exception as e:
        if auto_catch:
            log_error(__name__, sql_query, str(e))
        else:
            raise


def execute_cursor(sql_query, data=None, auto_catch=False):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        trace('%s --params-- %s' % (sql_query, data))
        cur.execute(sql_query, data)
        conn.commit()  # needed when we return the id of an insert for instance
        results = cur.fetchall()
        return results
    except Exception as e:
        if auto_catch:
            log_error(__name__, sql_query, str(e))
        else:
            raise


def execute_cursor_function(function_name, data=None, auto_catch=False):
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.callproc(function_name, data)
        conn.commit()
        rows = cur.fetchall()
        return rows
    except Exception as e:
        if auto_catch:
            log_error(__name__, function_name, str(e))
        else:
            raise


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


SESSION = None


def get_sqlalchemy_session(db_name=None):
    global SESSION
    if SESSION is None:
        log('creating new SQLAlchemy session', debug)
        Session = sessionmaker(bind=get_sqlalchemy_engine(db_name), autoflush=False)
        SESSION = Session()
    elif not SESSION.is_active:
        log('SQLAlchemy session is not active', debug)
        SESSION.rollback()
    return SESSION


def sqlalchemy_upsert(orm_object):
    # get_sqlalchemy_session().saveorupdate(orm_object)
    get_sqlalchemy_session().merge(orm_object)


def sqlalchemy_insert(orm_object):
    get_sqlalchemy_session().add(orm_object)


def sqlalchemy_commit():
    get_sqlalchemy_session().flush()
    get_sqlalchemy_session().commit()


def sqlalchemy_rollback():
    # cant use normal sesison getter as it woudl create a new session if not active (ie partial rollback mode)
    log('SQLAlchemy rollback initiated', debug)
    global SESSION
    if SESSION is not None:
        SESSION.rollback()


def sqlalchemy_close():
    log('SQLAlchemy close initiated', debug)
    get_sqlalchemy_session().close_all()


def sqlalchemy_flush():
    get_sqlalchemy_session().flush()

#
# if __name__ == "__main__":
#     # res = execute_cursor('select * from daily_data limit 10')
#     # print res
#
#     # large_query = "select * from daily_ema_dataset limit 1000000"
#     # query_to_file(large_query, 'large_query.csv')
#
#     filename = 'test_queries.csv'
#     query_to_file('select * from daily_data limit 100', filename)
#     execute_query('create table test_pyscilear as select * from daily_data where 1=2')
#     file_to_table('test_pyscilear', filename)
#     print(execute_scalar('select count(*) from test_pyscilear')[0])
#     execute_query('drop table test_pyscilear')
#
#     """
#     conn = get_pg_connection()
#     cur = conn.cursor()
#     cur.callproc("get_pending_jobs", ('news_sent', 10))
#
#     # named_cursor = conn.cursor(name='cursor_x')
#     for row in cur:
#         print row
#     """
