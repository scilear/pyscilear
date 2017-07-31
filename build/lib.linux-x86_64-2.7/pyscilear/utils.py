import json
import os
import pickle
import sys
import time
from datetime import datetime, timedelta

from logbook import Logger, debug, trace, warning, critical, catch_exceptions, exception, notice
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


PID = 0


def initialise_logging(file_name=None, stderr=True):
    global PID
    PID = os.getpid()
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
    if os.path.exists(r'/tmp/'):
        log_handler = FileHandler('/tmp/' + file_name + '_'
                                  + datetime.now().isoformat().replace(':', '-') + '.log', bubble=True)

    else:
        log_handler = FileHandler('c:\\temp\\' + file_name + '_'
                                  + datetime.now().isoformat().replace(':', '-') + '.log', bubble=True)

    log_handler.level = log_level
    log_handler.push_application()


def log(msg, verbosity=debug):
    enriched_msg = '[PID %s]: %s' % (PID, msg)
    if verbosity == debug:
        debug(enriched_msg)
    elif verbosity == trace:
        trace(enriched_msg)
    elif verbosity == info:
        info(enriched_msg)
    elif verbosity == warn:
        warn(enriched_msg)
    elif verbosity == warning:
        warning(enriched_msg)
    elif verbosity == notice:
        notice(enriched_msg)
    elif verbosity == error:
        error(enriched_msg)
    elif verbosity == exception:
        exception(enriched_msg)
    elif verbosity == catch_exceptions:
        catch_exceptions(enriched_msg)
    elif verbosity == critical:
        critical(enriched_msg)


def log_name(file_path):
    return os.path.splitext(os.path.basename(file_path))[0]


def not_yet_exit_time(hour, minute=0, weekday=None):
    now = datetime.today()
    if weekday is not None and now.weekday() < weekday:
        return True
    if now.hour < hour:
        return True
    elif now.hour > hour:
        return False
    else:
        return now.minute > minute


def func_log():
    # type: () -> object
    def decorator_func(func):
        def wrapper_func(*args, **kwargs):
            PY_VERSION = sys.version_info[0]
            if PY_VERSION == 2:
                func_name = func.func_name
            elif PY_VERSION == 3:
                func_name = func.__name__
            if len(args) > 0 and type(args[0]) == bool and args[0] != False:
                logger = Logger(args[0].__class__.__name__)
            else:
                logger = Logger(func_name)
            logger.debug('[PID %s]: +%s' % (PID, func_name))
            start_time = time.time()

            # Invoke the wrapped function first
            retval = func(*args, **kwargs)

            elapsed_time = time.time() - start_time
            logger.debug('[PID %s]: -%s - elasped %f s' % (PID, func_name, elapsed_time))
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
