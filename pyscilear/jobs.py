import multiprocessing

from gevent import sleep
from logbook import info
from psutil import Popen

from pyscilear.pg import upsert
import datetime


def mark_job(job_id, state):
    info('job %d is completed state = %s' % (job_id, state))
    upsert('jobs').row({'id': job_id}, {'state': state, 'done_processing': datetime.datetime.now()})


def kickoff_and_wait(python_file, args, cpu_count=None):
    if cpu_count is None:
        cpu_count = multiprocessing.cpu_count() - 1
    processes = []
    for i in range(0, cpu_count):
        # thread = threading.Thread(target=fly.NewsOptimizer.process_ticker_list, args=())
        # thread.setDaemon(True)
        # thread.start()
        arg_list = ['python', python_file] + args
        processes.append(Popen(arg_list))
        sleep(1)
    info('%d process spawn off for news polling' % cpu_count)
    for p in processes:
        p.wait()
    info('All spawn terminated')
