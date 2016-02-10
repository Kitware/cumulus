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

import cherrypy
import json
from pymongo import ReturnDocument

from girder.api.rest import RestException, getBodyJson, loadmodel,\
    getCurrentUser, getApiUrl, filtermodel, Resource
from girder.api import access
from girder.api.docs import addModel
from girder.api.describe import describeRoute, Description
from girder.constants import AccessType
import sys
from bson.objectid import ObjectId

from cumulus.taskflow import load_class, TaskFlowState, TaskState


class TaskFlows(Resource):

    def __init__(self):
        super(TaskFlows, self).__init__()
        self.resourceName = 'taskflows'
        self.route('POST', (), self.create)
        self.route('PATCH', (':id',), self.update)
        self.route('GET', (':id', 'status'), self.status)
        self.route('GET', (':id',), self.get)
        self.route('POST', (':id', 'log'), self.log)
        self.route('PUT', (':id', 'terminate'), self.terminate)
        self.route('PUT', (':id', 'start'), self.start)
        self.route('POST', (':id', 'tasks'), self.create_task)
        self.route('DELETE', (':id',), self.delete)
        self.route('PUT', (':id', 'delete'), self.delete_finished)
        self.route('GET', (':id','tasks'), self.tasks)
        self.route('PUT', (':id','tasks', ':taskId', 'finished'), self.task_finished)

        self._model = self.model('taskflow', 'taskflow')

    def _check_status(self, request):
        if request.status_code != 200:
            print >> sys.stderr, request.content
            request.raise_for_status()


    addModel('CreateTaskFlowParams', {
        'id': 'CreateTaskFlowParams',
        'required': ['taskFlowClass'],
        'properties': {
            'taskFlowClass': {
                'type': 'string'
            }
        }
    }, 'taskflows')

    @access.user
    @filtermodel(model='taskflow', plugin='taskflow')
    @describeRoute(
        Description('Create the taskflow')
        .param(
            'body',
            'The properties to update',
            required=False, paramType='body', dataType='CreateTaskFlowParams')
    )
    def create(self, params):
        user = self.getCurrentUser()
        taskflow = getBodyJson()

        self.requireParams(['taskFlowClass'], taskflow)

        # Check that we can load the class
        try:
            load_class(taskflow['taskFlowClass'])
        except:
            raise RestException(
                    'Unable to load taskflow class: %s' % taskflow['taskFlowClass'])
        taskflow = self._model.create(user, taskflow)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/taskflows/%s' % taskflow['_id']

        return taskflow

    @access.user
    @filtermodel(model='taskflow', plugin='taskflow')
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.WRITE)
    @describeRoute(
        Description('Update the taskflow')
        .param(
            'id',
            'The id of taskflow',
            required=True, paramType='path')
        .param(
            'updates',
            'The properties to update',
            required=True, paramType='body', dataType='object')
    )
    def update(self, taskflow, params):
        user = self.getCurrentUser()
        immutable = ['access', '_id', 'taskFlowClass', 'log', 'activeTaskCount']
        updates = getBodyJson()
        if not updates:
            raise RestException('A body must be provided', code=400)

        for p in updates:
            if p in immutable:
                raise RestException('\'%s\' is an immutable property' % p, 400)

        taskflow = self._model.update_taskflow(user, taskflow, updates)

        return taskflow

    def _status(self, user, taskflow):
        """
        Utility function to extract the status.
        """
        if 'status' in taskflow:
            if taskflow['status'] == TaskFlowState.TERMINATING and \
                taskflow['activeTaskCount'] == 0:
                return TaskFlowState.TERMINATED
            else:
                return taskflow['status']

        tasks = self.model('task', 'taskflow').find_by_taskflow_id(
            user, taskflow['_id'])

        task_status = [t['status'] for t in tasks]
        task_status = set(task_status)

        status = TaskFlowState.CREATED
        if len(task_status) ==  1:
            status = task_status.pop()
        elif TaskState.ERROR in task_status:
            status = TaskFlowState.ERROR
        elif TaskState.RUNNING in task_status or \
             (TaskState.COMPLETE in task_status and TaskState.CREATED in task_status):
            status = TaskFlowState.RUNNING

        return status

    @access.user
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.READ)
    @describeRoute(
        Description('Get the taskflow status')
        .param(
            'id',
            'The id of taskflow',
            required=True, paramType='path')
    )
    def status(self, taskflow, params):
        user = self.getCurrentUser()

        return {'status': self._status(user, taskflow)}


    @access.user
    @filtermodel(model='taskflow', plugin='taskflow')
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.READ)
    @describeRoute(
        Description('Get a taskflow')
        .param(
            'id',
            'The id of taskflow',
            required=True, paramType='path')
        .param(
            'path',
            'Option path to a particular property',
            required=False, paramType='query')
    )
    def get(self, taskflow, params):

        if 'path' in params:
            taskflow = self._model.get_path(taskflow, params['path'])

        return taskflow


    @access.user
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.WRITE)
    @describeRoute(None)
    def log(self, taskflow, params):
        body = cherrypy.request.body.read()
        if not body:
            raise RestException('Log entry must be provided', code=400)

        self._model.append_to_log(taskflow, json.loads(body))

    @access.user
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.WRITE)
    @describeRoute(
        Description('Terminate the taskflow ')
        .param(
            'id',
            'The id of taskflow',
            required=True, paramType='path')
    )
    def terminate(self, taskflow, params):
        user = getCurrentUser()
        taskflow['status'] = TaskFlowState.TERMINATING

        self._model.save(taskflow)
        constructor = load_class(taskflow['taskFlowClass'])
        token = self.model('token').createToken(user=user, days=7)
        taskflow = constructor(
            id=str(taskflow['_id']),
            girder_token=token['_id'],
            girder_api_url=getApiUrl())

        # Mark the taskflow as being used to termination
        taskflow['terminate'] = True

        taskflow.terminate()

    @access.user
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.ADMIN)
    @describeRoute(
        Description('Start the taskflow ')
        .param(
            'id',
            'The id of task',
            required=True, paramType='path')
    )
    def start(self, taskflow, params):
        user = self.getCurrentUser()


        constructor = load_class(taskflow['taskFlowClass'])
        token = self.model('token').createToken(user=user, days=7)

        workflow = constructor(
            id=str(taskflow['_id']),
            girder_token=token['_id'],
            girder_api_url=getApiUrl())

        workflow.start()

    @access.user
    @filtermodel(model='taskflow', plugin='taskflow')
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.ADMIN)
    @describeRoute(
        Description('Delete the taskflow')
        .param(
            'id',
            'The id of taskflow',
            required=True, paramType='path')
    )
    def delete(self, taskflow, params):
        user = self.getCurrentUser()

        if self._status == TaskFlowState.RUNNING:
            raise RestException('Taskflow is running', 400)

        constructor = load_class(taskflow['taskFlowClass'])
        token = self.model('token').createToken(user=user, days=7)

        if taskflow['status'] != TaskFlowState.DELETING:

            taskflow['status'] = TaskFlowState.DELETING
            self._model.save(taskflow)

            workflow = constructor(
                id=str(taskflow['_id']),
                girder_token=token['_id'],
                girder_api_url=getApiUrl())

            workflow.delete()

            # Check if we have any active tasks, it not then we are done and
            # can delete the tasks and taskflows
            taskflow = self._model.load(taskflow['_id'], user=user,
                                        level=AccessType.ADMIN)
            if taskflow['activeTaskCount'] == 0:
                self._model.delete(taskflow)
                cherrypy.response.status = 200
                taskflow['status'] = TaskFlowState.DELETED

                return taskflow

        cherrypy.response.status = 202
        return taskflow

    @access.user
    @filtermodel(model='task', plugin='taskflow')
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.READ)
    @describeRoute(
        Description('Get all the tasks associated with this taskflow')
        .param(
            'id',
            'The id of taskflow',
            required=True, paramType='path')
    )
    def tasks(self, taskflow, params):
        user = getCurrentUser()

        states = params.get('states')
        if states:
            states = json.loads(states)

        cursor = self.model('task', 'taskflow').find_by_taskflow_id(
            user, ObjectId(taskflow['_id']), states=states)

        return [task for task in cursor]

    addModel('CreateTaskParams', {
        'id': 'CreateTaskParams',
        'required': ['celeryTaskId'],
        'properties': {
            'celeryTaskId': {
                'type': 'string'
            }
        }
    }, 'taskflows')

    @access.user
    @filtermodel(model='task', plugin='taskflow')
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.READ)
    @describeRoute(
        Description('Create a new task associated with this flow')
        .param(
            'id',
            'The id of taskflow',
            required=True, paramType='path')
        .param(
            'body',
            'The properties to update',
            required=False, paramType='body', dataType='CreateTaskParams')
    )
    def create_task(self, taskflow, params):
        user = getCurrentUser()
        task = getBodyJson()

        self.requireParams(['celeryTaskId'], task)

        task = self.model('task', 'taskflow').create(user, taskflow, task)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/tasks/%s' % task['_id']

        return task

    @access.user
    @filtermodel(model='taskflow', plugin='taskflow')
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.WRITE)
    @describeRoute(None)
    def task_finished(self, taskflow, taskId, params):
        # decrement the number of active tasks
        query = {
            '_id': ObjectId(taskflow['_id'])
        }
        update = {
            '$inc': {
                'activeTaskCount': -1
            }
        }

        return self._model.collection.find_one_and_update(
            query, update, return_document=ReturnDocument.AFTER)

    @access.user
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.ADMIN)
    @describeRoute(None)
    def delete_finished(self, taskflow, params):
        self._model.delete(taskflow)


