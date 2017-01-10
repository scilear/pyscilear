import os
import pickle
import json
import sys

from datetime import datetime
import time

from logbook import Logger
from logbook import TRACE
from logbook import info, FileHandler, StreamHandler, error, warn, DEBUG


def get_log_level():
    log_level = DEBUG
    set_trace = False
    try:
        set_trace = os.environ['DEBUG_TRACE'] == 1
    except:
        pass
    if set_trace:
        log_level = TRACE
    return log_level

def initialise_logging(file_name=None, stderr=True):
    if file_name is None:
        file_name = 'python_log'
    else:
        base_name = os.path.basename(file_name)
        split_file = os.path.splitext(base_name)
        if split_file is not None and len(split_file) > 1:
            file_name = split_file[0]
        else:
            file_name = base_name
    log_level = get_log_level()

    StreamHandler(sys.stdout, bubble=True, level=log_level).push_application()
    if os.path.exists(r'/var/log/'):
        log_handler = FileHandler('/var/log/' + file_name + '_'
                                  + datetime.now().isoformat().replace(':', '-') + '.log', bubble=True)

    else:
        log_handler = FileHandler('c:\\logs\\' + file_name + '_'
                                  + datetime.now().isoformat().replace(':', '-') + '.log', bubble=True)

    log_handler.level = log_level
    log_handler.push_application()



def log_name(file_path):
    return os.path.splitext(os.path.basename(file_path))[0]


def func_log():
    # type: () -> object
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


@func_log()
def serialize(obj, file_name, format='pickle'):
    if format == 'pickle':
        s = pickle.dumps(obj)
        with open(file_name, 'wb') as pkl_file:
            pkl_file.write(s)
    elif format == 'json':
        with open(file_name, 'w', -1) as f:
            json.dump(obj, f)
    info('saved %s' % file_name)


@func_log()
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
