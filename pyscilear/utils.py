import os
import pickle
import sys

from datetime import datetime
from logbook import info, FileHandler, StreamHandler, error, warn

def initialise_logging(file_name=None, stderr=True):
    if file_name == None:
        file_name = 'python_log'
    StreamHandler(sys.stdout).push_application()
    if os.path.exists(r'/var/log/'):
        log_handler = FileHandler('/var/log/' + file_name + '_'
                                  + datetime.now().isoformat().replace(':', '-') + '.log', bubble=True)

    else:
        log_handler = FileHandler('c:\\logs\\' + file_name + '_'
                                  + datetime.now().isoformat().replace(':', '-') + '.log')

    log_handler.push_application()


def serialize(obj, file_name):
    s = pickle.dumps(obj)
    with open(file_name, 'wb') as pkl_file:
        pkl_file.write(s)
        info('saved %s' % file_name)


def deserialize( file_name):
    if os.path.exists(file_name):
        with open(file_name, 'rb') as pkl_file:
            s = pkl_file.read()
            obj = pickle.loads(s)
            return obj
    return None
