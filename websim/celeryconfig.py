from celery import Celery

_includes = (
    'websim.starcluster.tasks'
)
app = Celery('starcluster',  backend='amqp', broker='amqp://guest:guest@localhost:5672/',
             include=_includes)

app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=('json',),
    CELERY_RESULT_SERIALIZER = 'json'
)

