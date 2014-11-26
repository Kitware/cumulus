from __future__ import absolute_import
from celery import Celery

_includes = (
    'cumulus.starcluster.tasks.cluster',
    'cumulus.starcluster.tasks.job',
    'cumulus.moab.tasks.mesh'
)
app = Celery('starcluster',  backend='amqp', broker='amqp://guest:guest@localhost:5672/',
             include=_includes)

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=('json',),
    CELERY_RESULT_SERIALIZER = 'json'
)

