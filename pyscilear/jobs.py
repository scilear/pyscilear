import multiprocessing
import os
import sys
from time import sleep

import psutil
from logbook import info
from psutil import Popen

from pyscilear.pg import execute_query, execute_cursor_function, execute_scalar
import datetime
from pyscilear.utils import func_log


def create_job(job_group, info, start_processing=datetime.datetime.now()):
    #upsert('jobs').row({'job_group': job_group, 'info': info, 'state': 0, 'start_processing': start_processing})
    execute_query("insert into jobs (job_group, info, state, start_processing) "
                  "values "
                  "(%s, %s, %s, %s)", (job_group, info, 0, start_processing))


def is_primary_process(python_file):
    file_name = os.path.basename(python_file)
    pid = os.getpid()
    for process in psutil.process_iter():
        cmdline = process.cmdline()
        for p in cmdline:
            if file_name in p and pid > process.pid:
                return False
    return True

def create_job_batch(job_creator_group, sql):
    # to avoid concurrent update we create a job and process it,
    # it is better than locking the whole table as non related process may be using it
    # TODO a bit ugly, need to have some better system where get_pending_job of group resets to 0 global job
    # creation or something like that, might face same issues though as

    execute_query("""
    insert into jobs (job_group, info)
        select job_group, info from
        (select %s::varchar job_group, 'lock'::varchar info) new_job
        where not exists
        (select 1 from jobs j where j.job_group=new_job.job_group and (j.state<2 or j.done_processing>current_timestamp-INTERVAL '30 seconds'));
    """, (job_creator_group,))

    jobs = execute_cursor_function('get_pending_jobs', (job_creator_group, 1000000))
    done = False
    if len(jobs) == 0:
        return
    for job in jobs:
        if not done:
            execute_query(sql)
            done = True
        mark_job(job[0], 2)


def mark_job(job_id, state):
    info('job %d is completed state = %s' % (job_id, state))
    execute_query("update  jobs set state=%s, done_processing=%s where id=%s",
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
    info('%d process spawn off' % cpu_count)
    for p in processes:
        ret = p.wait()
        info('kickoff_and_wait: %s - %s ' % (str(p), str(ret)))
    info('All spawn terminated')
