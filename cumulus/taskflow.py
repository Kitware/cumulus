#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2016 Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the "License" );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################

from __future__ import absolute_import

from functools import wraps
import json
import threading

from girder_client import GirderClient, HttpError

import celery
from celery.signals import before_task_publish, task_postrun, task_prerun
from celery import current_task

from cumulus.celery import command

# We need to use thread local storage to save the "current user task", celery's
# current_task is not always what we need.
thread_local = threading.local()

# These key we use to store our taskflow data in the celery headers
TASKFLOW_HEADER = 'taskflow'
TASKFLOW_TASK_ID_HEADER = 'taskflow_task_id'

# The states that a taskflow can be in, there are likely to be more
class TaskState:
    RUNNING = 'running'
    ERROR = 'error'
    COMPLETE = 'complete'

def task(func):
    """
    Taskflow decortator used to inject the taskflow object as the first
    postional argument of the task. In the future probably also need to support
    providing the celery task as well.
    """
    # First apply our own wrapper
    @wraps(func)
    def wrapped(celery_task, *args, **kwargs):
        # Extract the taskflow from the headers
        taskflow = celery_task.request.headers[TASKFLOW_HEADER]
        taskflow = to_taskflow(taskflow)

        # Patch task state to 'running'
        client = GirderClient(apiUrl=taskflow.girder_api_url)
        client.token = taskflow.girder_token

        task_id = celery_task.request.headers[TASKFLOW_TASK_ID_HEADER]
        body = {
            'status': TaskState.RUNNING
        }
        url = 'tasks/%s' % task_id
        client.patch(url, data=json.dumps(body))

        # Now run the user task function
        return func(taskflow, *args, **kwargs)

    # Now apply the celery decorator
    celery_task = command.task(wrapped, bind=True)

    return celery_task

def to_taskflow(taskflow):
    """
    Utility method to ensure we have a taskflow instance rather than a simple
    dictionary.
    """
    if taskflow is not None:
        if isinstance(taskflow, dict):
            taskflow = TaskFlow(**taskflow)

    return taskflow

class TaskFlow(dict):
    """
    This is the base class users derive that taskflows from. In the future more
    utility methods can be added for example to update data/results associated
    with the taskflow.
    """
    def __init__(self, id=None, girder_token=None, girder_api_url=None):
        """
        Constructs a new TaskFlow instance
        """
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
        """
        This must be implement by subclass to give start of the taskflow.
        """
        raise NotImplemented('Must be implemented by subclass')

    def terminate(self):
        pass

    def delete(self):
        pass

    def run(self):
        self.start()

@task_prerun.connect
def task_prerun_handler(task_id=None, task=None, args=None, **kwargs):
    """
    This is called before a task it run. We use it the save the current task
    being run in thread local storage. We need this when celery uses built in
    tasks, such as 'celery.group', we want the 'user task'.

    For example:

    @taskflow.task
    def task4(workflow, *args, **kwargs):
        print 'task4'
        time.sleep(2)

        header = [task5.s() for i in range(10)]
        chord(header)(task6.s())

    In the above task when the chord is run it creates a group to run the header
    tasks in, when this gets executed, the current_task being an instance of
    'celery.group' rather than task4, we need to keep track that we are in
    task4.
    """
    thread_local.current_task = task


@before_task_publish.connect
def task_before_sent_handler(headers=None, body=None, **kwargs):
    """
    When a new task is being scheduled we need to create a corresponding
    taskflow task. We then store the task id the the request headers, so we
    can retrieve it in our decorator to move the taskflow task into the correct
    state, before executing the user function.
    """
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

    client = GirderClient(apiUrl=girder_api_url)
    client.token = girder_token

    # If this is a retry then we have already create a task get it from
    # the current tasks headers.
    if body['retries'] > 0:
        taskflow_task_id = current_task.request.headers[TASKFLOW_TASK_ID_HEADER]
    else:
        # This is a new task so create a taskflow task instance
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
    """
    Utility function to update the state of a given taskflow task.
    """
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
    """
    This handler extracts out the taskflow information from the request
    headers and the calls the appropriate handler based on the state of the
    celery task.
    """
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
    """
    Failure handler
    """
    # TODO to exception information

    # Now update the status
    _update_task_status(taskflow, taskflow_task_id, TaskState.ERROR)


def task_success_handler(taskflow, taskflow_task_id, celery_task, state=None,
                         args=None, **kwargs):
    """
    Success handler
    """
    # Update the status
    _update_task_status(taskflow, taskflow_task_id, TaskState.COMPLETE)

