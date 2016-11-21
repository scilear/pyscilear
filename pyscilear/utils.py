import os
import pickle
import json
import sys

from datetime import datetime
import time

from logbook import Logger
from logbook import info, FileHandler, StreamHandler, error, warn


def initialise_logging(file_name=None, stderr=True):
    if file_name == None:
        file_name = 'python_log'
    StreamHandler(sys.stdout, bubble=True).push_application()
    if os.path.exists(r'/var/log/'):
        log_handler = FileHandler('/var/log/' + file_name + '_'
                                  + datetime.now().isoformat().replace(':', '-') + '.log', bubble=True)

    else:
        log_handler = FileHandler('c:\\logs\\' + file_name + '_'
                                  + datetime.now().isoformat().replace(':', '-') + '.log', bubble=True)

    log_handler.push_application()


def func_log():
    def decorator_func(func):
        def wrapper_func(*args, **kwargs):
            if len(args) > 0:
                logger = Logger(args[0].__class__.__name__)
            else:
                logger = Logger(func.func_name)
            logger.debug('+%s' % func.func_name)
            start_time = time.time()

            # Invoke the wrapped function first
            retval = func(*args, **kwargs)

            elapsed_time = time.time() - start_time
            logger.debug('-%s - elasped %f s' % (func.func_name, elapsed_time))
            return retval

        return wrapper_func

    return decorator_func


def serialize(obj, file_name, format='pickle'):
    if format == 'pickle':
        s = pickle.dumps(obj)
        with open(file_name, 'wb') as pkl_file:
            pkl_file.write(s)
    elif format == 'json':
        with open(file_name, 'w') as f:
            json.dump(obj, f)
    info('saved %s' % file_name)


def deserialize(file_name, format='pickle'):
    if format == 'pickle':
        if os.path.exists(file_name):
            with open(file_name, 'rb') as pkl_file:
                s = pkl_file.read()
                obj = pickle.loads(s)
                return obj
    elif format == 'json':
        if os.path.exists(file_name):
            with open(file_name, 'rb') as f:
                obj = json.load(f)
                return obj
    return None


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def daterange_weekdays(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        new_date = start_date + timedelta(n)
        if new_date.weekday() >= 5:
            continue
        yield new_date

