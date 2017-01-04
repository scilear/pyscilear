import multiprocessing

from gevent import sleep
from logbook import info
from psutil import Popen

from pyscilear.pg import upsert
import datetime
from utils import func_log


def create_job(job_group, info):
    upsert('jobs').row({'job_group': job_group, 'info': info})


def mark_job(job_id, state):
    info('job %d is completed state = %s' % (job_id, state))
    upsert('jobs').row({'id': job_id}, {'state': state, 'done_processing': datetime.datetime.now()})


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
        arg_list = ['python', python_file] + args_cpy
        processes.append(Popen(arg_list))
        sleep(1)
    info('%d process spawn off for news polling' % cpu_count)
    for p in processes:
        ret = p.wait()
        info('kickoff_and_wait: %s - %s ' % (str(p), str(ret)))
    info('All spawn terminated')
