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
import logging
import importlib

from functools import wraps
import json
import threading

from girder_client import GirderClient, HttpError

from celery.signals import before_task_publish, task_prerun, task_failure, \
    task_success

from celery.canvas import maybe_signature
from celery import current_task
from celery.utils.log import get_task_logger

import cumulus.celery
from cumulus.logging import RESTfulLogHandler


logger = get_task_logger(__name__)


# We need to use thread local storage to save the "current user task", celery's
# current_task is not always what we need.
thread_local = threading.local()

# These key we used to store our taskflow data in the celery headers
TASKFLOW_HEADER = 'taskflow'
TASKFLOW_TASK_ID_HEADER = 'taskflow_task_id'
TASKFLOW_RETRY_HEADER = 'taskflow_retries'


# The states that a taskflow can be in, there are likely to be more
class TaskState:
    CREATED = 'created'
    RUNNING = 'running'
    ERROR = 'error'
    COMPLETE = 'complete'


class TaskFlowState:
    CREATED = 'created'
    RUNNING = 'running'
    ERROR = 'error'
    UNEXPECTEDERROR = 'unexpectederror'
    TERMINATING = 'terminating'
    TERMINATED = 'terminated'
    DELETING = 'deleting'
    DELETED = 'deleted'
    COMPLETE = 'complete'


def _get_task_logger(id, girder_api_url, girder_token):
    logger = logging.getLogger('task.%s' % id)
    logger.setLevel(logging.INFO)
    # Only add new new handler if we don't already have one.
    if not logger.handlers:
        url = '%s/tasks/%s/log' % (girder_api_url, id)
        logger.addHandler(RESTfulLogHandler(girder_token, url))

    return logger


def _create_girder_client(girder_api_url, girder_token):
    client = GirderClient(apiUrl=girder_api_url)
    client.token = girder_token

    return client


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
        task_id = celery_task.request.headers[TASKFLOW_TASK_ID_HEADER]

        # Get the current state of the taskflow so we know if we are terminating
        # Only do this if this task is not associated with termination ...
        if 'terminate' not in taskflow or not taskflow['terminate']:
            status = taskflow.status()
            if status == TaskFlowState.TERMINATING or \
                    status == TaskFlowState.UNEXPECTEDERROR:
                return

        setattr(celery_task, 'taskflow', taskflow)
        setattr(celery_task, 'logger', _get_task_logger(
            task_id, taskflow.girder_api_url, taskflow.girder_token))

        return func(celery_task, *args, **kwargs)

    # Now apply the celery decorator
    celery_task = cumulus.celery.command.task(
        wrapped, bind=True)

    return celery_task


def load_class(class_name):
    module, cls = class_name.rsplit('.', 1)
    try:
        imported = importlib.import_module(module)
    except ImportError:
        raise

    try:
        constructor = getattr(imported, cls)
    except AttributeError:
        raise

    return constructor


def to_taskflow(taskflow):
    """
    Utility method to ensure we have a taskflow instance rather than a simple
    dictionary.
    """
    if taskflow is not None:
        if isinstance(taskflow, dict):
            constr = load_class(taskflow['_type'])
            taskflow = constr(**taskflow)

    return taskflow


class TaskFlow(dict):
    """
    This is the base class users derive that taskflows from. In the future more
    utility methods can be added for example to update data/results associated
    with the taskflow.
    """
    def __init__(self, id=None, girder_token=None,
                 girder_api_url=None, **kwargs):
        """
        Constructs a new TaskFlow instance
        """
        super(TaskFlow, self).__init__(
            girder_api_url=girder_api_url,
            girder_token=girder_token,
            id=id, **kwargs)
        cls = self.__class__
        self['_type'] = '%s.%s' % (cls.__module__, cls.__name__)

        self.logger = logging.getLogger('taskflow.%s' % id)
        self.logger.setLevel(logging.INFO)
        # Only add new new handler if we don't already have one.
        if not self.logger.handlers:
            url = '%s/taskflows/%s/log' % (girder_api_url, id)
            self.logger.addHandler(RESTfulLogHandler(girder_token, url))

    @property
    def id(self):
        return self['id']

    @property
    def girder_token(self):
        return self['girder_token']

    @property
    def girder_api_url(self):
        return self['girder_api_url']

    def run_task(self, signature, **options):
        """
        Add appropriate headers and run task
        """
        signature.apply_async(
            headers={
                TASKFLOW_HEADER: self
            }, **options)

    def start(self, signature, **options):
        """
        This must be called by subclass to give start to the taskflow.
        """
        self.run_task(signature, **options)

    def terminate(self):
        pass

    def delete(self):
        pass

    def run(self):
        self.start()

    def set_metadata(self, key, value):
        """
        Set metadata on the taskflow. This can be used to save results or other
        output.

        :params key: The value key.
        :params value: The value.
        """
        girder_token = self['girder_token']
        girder_api_url = self['girder_api_url']

        client = _create_girder_client(girder_api_url, girder_token)
        url = 'taskflows/%s' % self.id
        body = {
            'meta.%s' % key: value
        }
        client.patch(url, data=json.dumps(body))

    def get_metadata(self, key):
        """
        Get metadata from the taskflow.

        :params key: The value key.
        """
        girder_token = self['girder_token']
        girder_api_url = self['girder_api_url']

        client = _create_girder_client(girder_api_url, girder_token)
        url = 'taskflows/%s' % self.id
        params = {
            'path': 'meta.%s' % key
        }
        r = client.get(url, parameters=params)

        return r['meta']

    class _on_complete_instance(object):
        """
        Private utility class to enable the on_complete syntax
        """
        def __init__(self, taskflow, task):
            self._taskflow = taskflow
            self._completed_task = task.name

        def run(self, task_to_run):
            """
            Called with the follow on task
            """
            # Register the callback with the taskflow
            self._taskflow._register_on_complete(self._completed_task,
                                                 task_to_run)

    def _register_on_complete(self, complete_task, task_to_run):
        """
        Private utility method to register an on complete callback
        """
        on_complete_map = self.setdefault('_on_complete_map', {})
        on_complete_map[complete_task] = task_to_run

    def _on_complete_lookup(self, completed_task_name):
        """
        Private utility method to lookup can callback that may be registered
        for a give completed task.
        """
        on_complete_map = self.setdefault('_on_complete_map', {})
        to_run = on_complete_map.get(completed_task_name)

        if to_run:
            # Convert back to celery signature object
            to_run = maybe_signature(to_run)

        return to_run

    def on_complete(self, task):
        """
        This method allow a follow on task to register, that will be run when
        an certain task is complete.

        The syntax is a follows:

        taskflow.on_complete(completed_task).run(task_to_run.s())
        """
        return TaskFlow._on_complete_instance(self, task)

    def connect(self, taskflow):
        """
        This method allow two taskflow to be connected together to form a
        composite flow.
        """
        self.setdefault(['_next'], []).append(taskflow)

    def status(self):
        """
        Return the current status of this taskflow
        """
        girder_token = self['girder_token']
        girder_api_url = self['girder_api_url']
        client = _create_girder_client(girder_api_url, girder_token)
        url = 'taskflows/%s/status' % self.id
        r = client.get(url)

        return r['status']


class CompositeTaskFlow(TaskFlow):
    TASKFLOWS = '_taskflows'

    def add(self, taskflow):
        taskflows = self.setdefault(CompositeTaskFlow.TASKFLOWS, [])
        taskflows.append(taskflow)

    def start(self):
        taskflow = self[CompositeTaskFlow.TASKFLOWS].pop(0)
        taskflow[CompositeTaskFlow.TASKFLOWS] = \
            self[CompositeTaskFlow.TASKFLOWS]

        taskflow.start()


def _taskflow_task_finished(taskflow, taskflow_task_id):
    girder_token = taskflow['girder_token']
    girder_api_url = taskflow['girder_api_url']

    client = GirderClient(apiUrl=girder_api_url)
    client.token = girder_token
    url = 'taskflows/%s/tasks/%s/finished' % (taskflow.id, taskflow_task_id)

    return client.put(url)


@task_prerun.connect
def task_prerun_handler(task_id=None, task=None, args=None, **kwargs):
    """
    This is called before a task is run. We use it the save the current task
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

    if TASKFLOW_HEADER in task.request.headers:
        taskflow = task.request.headers[TASKFLOW_HEADER]
        taskflow = to_taskflow(taskflow)
        task_id = task.request.headers[TASKFLOW_TASK_ID_HEADER]

        # Patch task state to 'running'
        _update_task_status(taskflow, task_id, TaskState.RUNNING)

    thread_local.current_task = task


@before_task_publish.connect
def task_before_sent_handler(headers=None, body=None, **kwargs):
    """
    When a new task is being scheduled we need to create a corresponding
    taskflow task. We then store the task id the the request headers, so we
    can retrieve it in our decorator to move the taskflow task into the correct
    state, before executing the user function.
    """
    # Only track tasks that have taskflow as apart of the routing_key
    # Note: We also check for the existence of thread_local.current_task
    # this is because internally, chords call the group function directly
    # rather than launching an async celery.group task.
    # (See: https://github.com/celery/celery/blob/v3.1.20/ ( line wrapped )
    #  celery/app/builtins.py#L335-L337)
    # This means the before_task_publish signal is never called. Without the
    # before_task_publish signal the group task never recieves the
    # TASKFLOW_HEADER information and the 'reduce' task doesn't have
    # access to the taskflow header information

#    if kwargs['routing_key'].startswith('taskflow') or \
#       hasattr(thread_local, 'current_task'):

    # This will only be true for the initial task called
    # from the taskflow object.
    if not hasattr(thread_local, 'current_task'):
        thread_local.current_task = None

    def _update_girder(taskflow, body):
        taskflow = to_taskflow(taskflow)
        taskflow_id = taskflow['id']
        girder_token = taskflow['girder_token']
        girder_api_url = taskflow['girder_api_url']

        client = GirderClient(apiUrl=girder_api_url)
        client.token = girder_token

        client = _create_girder_client(girder_api_url, girder_token)

        # If this is a retry then we have already create a task get it from
        # the current tasks headers.
        if body['retries'] > 0:
            taskflow_task_id = \
                current_task.request.headers[TASKFLOW_TASK_ID_HEADER]

            # Celery always fires the postrun handler with a state of SUCCESS
            # for retries. So we need to save the retries here so we can
            # determine in the postrun handler if the task is really complete.
            current_task.request.headers[TASKFLOW_RETRY_HEADER] \
                = body['retries']
        else:
            # This is a new task so create a taskflow task instance
            body = {
                'celeryTaskId': body['id'],
                'name': body['task']
            }
            url = 'taskflows/%s/tasks' % taskflow_id
            r = client.post(url, data=json.dumps(body))
            taskflow_task_id = r['_id']
        return taskflow, taskflow_task_id

    # First task in the queue
    if headers is not None and TASKFLOW_HEADER in headers:
        taskflow, taskflow_task_id = _update_girder(
            headers[TASKFLOW_HEADER], body)
        headers[TASKFLOW_TASK_ID_HEADER] = taskflow_task_id
        headers[TASKFLOW_HEADER] = taskflow
    # All other tasks
    elif thread_local.current_task is not None and \
            TASKFLOW_HEADER in thread_local.current_task.request.headers:

        taskflow, taskflow_task_id = _update_girder(
            thread_local.current_task.request.headers[TASKFLOW_HEADER], body)
        headers[TASKFLOW_TASK_ID_HEADER] = taskflow_task_id
        headers[TASKFLOW_HEADER] = taskflow
        # Save the task_id and taskflow in the headers
    else:
        print(body['task'])


def _update_task_status(taskflow, task_id, status):
    """
    Utility function to update the state of a given taskflow task.
    """
    girder_token = taskflow['girder_token']
    girder_api_url = taskflow['girder_api_url']

    url = 'tasks/%s' % task_id
    client = _create_girder_client(girder_api_url, girder_token)
    body = {
        'status': status
    }
    client.patch(url, data=json.dumps(body))


def _update_taskflow_status(taskflow, status):
    """
    Utility function to update the state of a given taskflow.
    """
    girder_token = taskflow['girder_token']
    girder_api_url = taskflow['girder_api_url']

    url = 'taskflows/%s' % taskflow.id
    client = _create_girder_client(girder_api_url, girder_token)
    body = {
        'status': status
    }
    client.patch(url, data=json.dumps(body))


@task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None,
                         traceback=None, **kwargs):
    if TASKFLOW_HEADER in sender.request.headers:
        taskflow_task_id = None
        try:
            headers = sender.request.headers
            taskflow_task_id = headers[TASKFLOW_TASK_ID_HEADER]
            taskflow = headers[TASKFLOW_HEADER]
            taskflow = to_taskflow(taskflow)

            _taskflow_task_finished(taskflow, taskflow_task_id)

            # Now update the status
            _update_task_status(taskflow, taskflow_task_id, TaskState.ERROR)

            logger = _get_task_logger(
                taskflow_task_id, taskflow.girder_api_url,
                taskflow.girder_token)

            msg = 'Exception raise by task.'
            logger.error(
                msg, exc_info=[type(exception), exception, traceback])

        except HttpError as ex:
            if taskflow_task_id:
                _update_taskflow_status(taskflow, TaskFlowState.UNEXPECTEDERROR)
                msg = 'An HttpError was raise in task_failure_handler for ' \
                      'task \'%s\' the response text was:\n%s' % \
                      (taskflow_task_id, ex.responseText)
                taskflow.logger.exception(msg)
                logger.error(msg)
            raise

        except Exception:
            if taskflow_task_id:
                _update_taskflow_status(taskflow, TaskFlowState.UNEXPECTEDERROR)
                taskflow.logger.exception(
                    'An exception was raise in task_failure_handler for '
                    'task \'%s\'' % taskflow_task_id)
            raise


@task_success.connect
def task_success_handler(sender=None, **kwargs):
    if TASKFLOW_HEADER in sender.request.headers:
        taskflow_task_id = None
        try:
            # Extract the taskflow from the headers
            headers = sender.request.headers
            taskflow = headers[TASKFLOW_HEADER]
            taskflow = to_taskflow(taskflow)
            taskflow_task_id = headers[TASKFLOW_TASK_ID_HEADER]

            # Get the number of retries saved in the before_send
            taskflow_retries \
                = current_task.request.get('headers',
                                           {}).get(TASKFLOW_RETRY_HEADER)

            # If the retry counts don't match then we know we have been
            # reschedule so we shouldn't mark the task as complete.
            if taskflow_retries and taskflow_retries != sender.request.retries:
                return

            # See if we have any follow on tasks
            to_run = taskflow._on_complete_lookup(sender.name)
            # Only run follow on tasks if we aren't terminating
            if to_run and taskflow.status() != TaskFlowState.TERMINATING:
                to_run.delay()

            # Is the completion of this task going to complete the flow?
            # Signal to the taskflow that we are finished and see what the
            # active task count is. As the decrement of the activeTaskCount
            # property is atomic everything should be in sync. If the count has
            # dropped to zero we know that this task finishes this flow so if
            # we have a connected flow we should start it.
            deleted = False
            r = _taskflow_task_finished(taskflow, taskflow_task_id)
            if r['activeTaskCount'] == 0:
                taskflow = to_taskflow(taskflow)

                status = taskflow.status()

                if CompositeTaskFlow.TASKFLOWS in taskflow and \
                        taskflow[CompositeTaskFlow.TASKFLOWS]:

                    if status != TaskFlowState.TERMINATING:
                        taskflows = taskflow[CompositeTaskFlow.TASKFLOWS]
                        next_taskflow = to_taskflow(taskflows.pop(0))
                        # Only run each flow once ...
                        taskflow_header \
                            = sender.request.headers[TASKFLOW_HEADER]
                        taskflow_header[CompositeTaskFlow.TASKFLOWS] = taskflows
                        # Also update the taskflow type to match the new flow
                        sender.request.headers[TASKFLOW_HEADER]['_type'] \
                            = next_taskflow['_type']
                        next_taskflow.start()

                # If we are finished deleting, do the final clean up
                if status == TaskFlowState.DELETING:
                    client = _create_girder_client(
                        taskflow.girder_api_url, taskflow.girder_token)
                    url = 'taskflows/%s/delete' % taskflow.id
                    r = client.put(url)
                    deleted = True

            # Update the status
            if not deleted:
                _update_task_status(
                    taskflow, taskflow_task_id, TaskState.COMPLETE)

        except HttpError as ex:
            if taskflow_task_id:
                _update_taskflow_status(taskflow, TaskFlowState.UNEXPECTEDERROR)
                msg = 'An HttpError was raise in task_success_handler for ' \
                      'task \'%s\' the response text was:\n%s' % \
                      (taskflow_task_id, ex.responseText)
                taskflow.logger.exception(msg)
                logger.error(msg)
            raise

        except Exception:
            if taskflow_task_id:
                _update_taskflow_status(taskflow, TaskFlowState.UNEXPECTEDERROR)
                taskflow.logger.exception(
                    'An exception was raise in task_success_handler for '
                    'task \'%s\'' % taskflow_task_id)
            raise
