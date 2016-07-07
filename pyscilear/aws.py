import os

import pandas as pd
import psycopg2
import sys
from boto.s3.connection import S3Connection
from logbook import info, debug


def get_access_keys():
    S3_ACCESS_KEY = ''
    S3_SECRET_KEY = ''
    if os.path.exists('access_keys'):
        ak = pd.read_csv('access_keys')
        S3_ACCESS_KEY = ak.values[0][0]
        S3_SECRET_KEY = ak.values[0][1]
        debug(ak)
    else:
        S3_ACCESS_KEY = os.environ['S3_ACCESS_KEY']
        S3_SECRET_KEY = os.environ['S3_SECRET_KEY']
    return S3_ACCESS_KEY, S3_SECRET_KEY


def get_db_access():
    ak = pd.read_csv('db_access')
    db_name = ak.values[0][0]
    db_user = ak.values[0][1]
    db_password = ak.values[0][2]
    db_host = ak.values[0][3]
    debug(ak)
    return db_name, db_user, db_password, db_host


def get_redshift_connection():
    db, user, pwd, host = get_db_access()
    conn_string = "dbname='%s' port='5439' user='%s' password='%s' host='%s'" % (db, user, pwd, host);
    debug(conn_string)
    conn = psycopg2.connect(conn_string)
    return conn


def execute_query(sql_query):
    conn = get_redshift_connection()
    cur = conn.cursor()
    debug(sql_query)
    cur.execute(sql_query)
    conn.commit()


def execute_scalar(sql_query, data=None):
    conn = get_redshift_connection()
    cur = conn.cursor()
    debug(sql_query)
    cur.execute(sql_query, data)
    results = cur.fetchone()
    return results


def execute_cursor(sql_query, data=None):
    conn = get_redshift_connection()
    cur = conn.cursor()
    debug(sql_query)
    cur.execute(sql_query, data)
    results = cur.fetchall()
    return results


def load_s3_file(file, bucket_name, table_name):
    conn = get_redshift_connection()
    cur = conn.cursor()
    key, secret = get_access_keys()
    sql_copy = "copy %s from 's3://%s/%s' " \
               "credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' csv;" \
               % (table_name, bucket_name, file, key, secret)
    debug(sql_copy)
    cur.execute(sql_copy)
    conn.commit()


def get_s3_connection():
    access_key, secret_key = get_access_keys()
    conn = S3Connection(access_key, secret_key)
    return conn


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()


def s3_put_file_in_bucket(file, bucket_name):
    s3conn = get_s3_connection()
    bucket = s3conn.get_bucket(bucket_name)
    file_name = os.path.basename(file)
    bucket.new_key(file_name).set_contents_from_filename(file, cb=percent_cb, num_cb=50)
    sys.stdout.write('\n')
    sys.stdout.flush()


def s3_get_file_from_bucket(file_name, bucket_name):
    s3conn = get_s3_connection()
    bucket = s3conn.get_bucket(bucket_name)
    bucket.new_key(file_name).get_contents_to_filename(file_name)
    sys.stdout.write('\n')
    sys.stdout.flush()
