from __future__ import absolute_import

from functools import wraps
import json
import threading

from girder_client import GirderClient, HttpError

import celery
from celery.signals import before_task_publish, task_postrun, task_prerun
from celery import current_task

from cumulus.celery import command

thread_local = threading.local()

TASKFLOW_HEADER = 'taskflow'
TASKFLOW_TASK_ID_HEADER = 'taskflow_task_id'

class TaskState:
    RUNNING = 'running'
    ERROR = 'error'
    COMPLETE = 'complete'

def task(func):

    # First apply our own wrapper
    @wraps(func)
    def wrapped(celery_task, *args, **kwargs):
        # Extract the taskflow from the headers
        taskflow = celery_task.request.headers[TASKFLOW_HEADER]
        taskflow = to_taskflow(taskflow)

        # Patch task state
        client = GirderClient(apiUrl=taskflow.girder_api_url)
        client.token = taskflow.girder_token

        task_id = celery_task.request.headers[TASKFLOW_TASK_ID_HEADER]
        body = {
            'status': TaskState.RUNNING
        }
        url = 'tasks/%s' % task_id
        client.patch(url, data=json.dumps(body))

        return func(taskflow, *args, **kwargs)

    # Now apply the celery decorator
    celery_task = command.task(wrapped, bind=True)

    return celery_task

def to_taskflow(d, app=None):
    if d is not None:
        if isinstance(d, dict):
            d = TaskFlow(d)

    return d

class TaskFlow(dict):
    def __init__(self, d=None, id=None, girder_token=None, girder_api_url=None):

        if isinstance(d, dict):
            super(TaskFlow, self).__init__(d)
        else:
            super(TaskFlow, self).__init__(
                girder_api_url=girder_api_url,
                girder_token=girder_token,
                id=id)

    @property
    def id(self):
        return self['id']

    @property
    def girder_token(self):
        return self['girder_token']

    @property
    def girder_api_url(self):
        return self['girder_api_url']

    # State methods
    def start(self):
        pass

    def terminate(self):
        pass

    def delete(self):
        pass

    def run(self):
        self.start()

@task_prerun.connect
def task_prerun_handler(task_id=None, task=None, args=None, **kwargs):
    thread_local.current_task = task


@before_task_publish.connect
def task_before_sent_handler(headers=None, body=None, **kwargs):

    # If we are not in a task then we are at the start of a taskflow
    # so we need to extract the taskflow object from the first postional
    # arg
    if not current_task:
        args = body['args']
        taskflow = to_taskflow(args[0])
    else:
        # If the current task is one of celery's internal tasks, such as
        # celery.group, the context will not contain the headers we need, so
        # instead use the task we stored in thread local.
        if current_task.request.headers:
            taskflow = current_task.request.headers[TASKFLOW_HEADER]
            taskflow = to_taskflow(taskflow)
        else:
            taskflow = thread_local.current_task.request.headers[TASKFLOW_HEADER]
            taskflow = to_taskflow(taskflow)

    taskflow_id = taskflow['id']

    girder_token = taskflow['girder_token']
    girder_api_url = taskflow['girder_api_url']

    # Create a task instance
    client = GirderClient(apiUrl=girder_api_url)
    client.token = girder_token

    # If this is a retry then we have already create a task get it from
    # the current tasks headers.
    if body['retries'] > 0:
        taskflow_task_id = current_task.request.headers[TASKFLOW_TASK_ID_HEADER]
    else:
        body = {
            'celeryTaskId': body['id'],
            'name': body['task']
        }
        url = 'taskflows/%s/tasks' % taskflow_id
        r = client.post(url, data=json.dumps(body))
        taskflow_task_id = r['_id']

    # Save the task_id and taskflow in the headers
    headers[TASKFLOW_TASK_ID_HEADER] = taskflow_task_id
    headers[TASKFLOW_HEADER] = taskflow

def _update_task_status(taskflow, task_id, status):
    girder_token = taskflow['girder_token']
    girder_api_url = taskflow['girder_api_url']

    url = 'tasks/%s' % task_id
    client = GirderClient(apiUrl=girder_api_url)
    client.token = girder_token
    body = {
        'status': status
    }
    client.patch(url, data=json.dumps(body))

# Here we use postrun instead of on success or failure as we need original task
@task_postrun.connect
def task_postrun_handler(task=None, state=None, args=None, **kwargs):
        # Extract the taskflow from the headers
        taskflow = task.request.headers[TASKFLOW_HEADER]
        taskflow = to_taskflow(taskflow)
        taskflow_task_id = task.request.headers[TASKFLOW_TASK_ID_HEADER]

        if state == celery.states.SUCCESS:
            task_success_handler(taskflow, taskflow_task_id, task, state, args)
        elif state == celery.states.FAILURE:
            task_failure_handler(taskflow, taskflow_task_id, task, **kwargs)

def task_failure_handler(taskflow, taskflow_task_id, celery_task,
                         exception=None, args=None, kwargs=None,
                         traceback=None, einfo=None, **extra):
    # TODO to exception information

    # Now update the status
    _update_task_status(taskflow, taskflow_task_id, TaskState.ERROR)


def task_success_handler(taskflow, taskflow_task_id, celery_task, state=None,
                         args=None, **kwargs):

    # Update the status
    _update_task_status(taskflow, taskflow_task_id, TaskState.COMPLETE)

