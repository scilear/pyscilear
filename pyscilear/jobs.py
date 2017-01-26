import multiprocessing
import os
import sys
from time import sleep
from logbook import info
from psutil import Popen

from pyscilear.pg import execute_query
import datetime
from pyscilear.utils import func_log


def create_job(job_group, info, start_processing=datetime.datetime.now()):
    #upsert('jobs').row({'job_group': job_group, 'info': info, 'state': 0, 'start_processing': start_processing})
    execute_query("insert into jobs (job_group, info, state, start_processing) "
                  "values "
                  "(%s, %s, %s, %s)", (job_group, info, 0, start_processing))


def mark_job(job_id, state):
    info('job %d is completed state = %s' % (job_id, state))
    execute_query("update  jobs set state=%s, done_processing=%s where job_id=%s",
                  (state, datetime.datetime.now(), job_id))
    #upsert('jobs').row({'id': job_id}, {'state': state, 'done_processing': datetime.datetime.now()})


@func_log()
def kickoff_and_wait(python_file, args=[], cpu_count=None):
    if cpu_count is None:
        cpu_count = multiprocessing.cpu_count() - 1

    processes = []
    for i in range(0, cpu_count):
        args_cpy = list(args)
        for k, arg in enumerate(args):
            if arg == '{$index$}':
                args_cpy[k] = str(i)
            elif arg == '{$cpu_count$}':
                args_cpy[k] = str(cpu_count)

        if os.name == 'nt':
            python_executable = 'python'
        else:
            PY_VERSION = sys.version_info[0]
        if PY_VERSION == 2: 
                python_executable = 'python'
        elif PY_VERSION == 3: 
                python_executable = 'python3'

        arg_list = [python_executable, python_file] + args_cpy
        processes.append(Popen(arg_list))
        sleep(1)
    info('%d process spawn off for news polling' % cpu_count)
    for p in processes:
        ret = p.wait()
        info('kickoff_and_wait: %s - %s ' % (str(p), str(ret)))
    info('All spawn terminated')
