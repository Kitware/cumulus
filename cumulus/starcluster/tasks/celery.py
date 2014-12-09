from __future__ import absolute_import
from celery import Celery

_includes = (
    'cumulus.starcluster.tasks.cluster',
    'cumulus.starcluster.tasks.job',
    'cumulus.moab.tasks.mesh',
    'cumulus.task.status'
)

# Route short tasks to their own queue
routes = {
    'cumulus.starcluster.tasks.job.monitor_job': {
        'queue': 'monitor'
    },
    'cumulus.starcluster.tasks.job.monitor_process': {
        'queue': 'monitor'
    },
    'cumulus.task.status.monitor_status': {
        'queue': 'monitor'
    }
}

app = Celery('starcluster',  backend='amqp', broker='amqp://guest:guest@localhost:5672/',
             include=_includes)

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=('json',),
    CELERY_RESULT_SERIALIZER = 'json',
    CELERY_ACKS_LATE=True,
    CELERYD_PREFETCH_MULTIPLIER=1,
    CELERY_ROUTES=routes
)

