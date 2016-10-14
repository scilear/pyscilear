from logbook import info

from pyscilear.pg import upsert
import datetime

def mark_job(job_id, state):
    info('job %d is completed state = %s' % (job_id, state))
    upsert('jobs').row({'id': job_id}, {'state': state, 'done_processing':datetime.datetime.now()})
