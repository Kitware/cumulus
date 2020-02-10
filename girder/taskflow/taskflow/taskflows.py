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
import traceback
import logging

from girder.api.rest import RestException, getBodyJson, loadmodel,\
    getCurrentUser, filtermodel, Resource
from girder.api import access
from girder.api.docs import addModel
from girder.api.describe import describeRoute, Description
from girder.constants import AccessType, TokenScope
from girder.utility.model_importer import ModelImporter
import sys
from bson.objectid import ObjectId

from cumulus.taskflow import load_class, TaskFlowState
import cumulus

logger = logging.getLogger('girder')


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
        self.route('GET', (':id', 'access'), self.get_access)
        self.route('PUT', (':id', 'access'), self.set_access)
        self.route('PATCH', (':id', 'access'), self.patch_access)
        self.route('PATCH', (':id', 'access', 'revoke'), self.revoke_access)
        self.route('GET', (':id', 'tasks'), self.tasks)
        self.route('PUT', (':id', 'tasks', ':taskId', 'finished'),
                   self.task_finished)
        self.route('GET', (':id', 'log'), self.get_log)

        self._model = ModelImporter.model('taskflow', 'taskflow')

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

    @access.user(scope=TokenScope.DATA_WRITE)
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
        except Exception as ex:
            msg = 'Unable to load taskflow class: %s (%s)' % \
                  (taskflow['taskFlowClass'], ex)
            logger.exception(msg)
            traceback.print_exc()
            raise RestException(msg, 400)
        taskflow = self._model.create(user, taskflow)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/taskflows/%s' % \
                                                taskflow['_id']

        return taskflow

    @access.user(scope=TokenScope.DATA_WRITE)
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

    @access.user(scope=TokenScope.DATA_READ)
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.READ)
    @describeRoute(
        Description('Get the taskflow status')
        .param(
            'id',
            'The id of taskflow',
            required=True, paramType='path')
    )
    def status(self, taskflow, params):
        return {'status': taskflow['status']}

    @access.user(scope=TokenScope.DATA_READ)
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

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.WRITE)
    @describeRoute(None)
    def log(self, taskflow, params):
        body = cherrypy.request.body.read().decode('utf8')
        if not body:
            raise RestException('Log entry must be provided', code=400)

        self._model.append_to_log(taskflow, json.loads(body))

    @access.user(scope=TokenScope.DATA_WRITE)
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
        token = ModelImporter.model('token').createToken(user=user, days=7)
        taskflow_instance = constructor(
            id=str(taskflow['_id']),
            girder_token=token['_id'],
            girder_api_url=cumulus.config.girder.baseUrl)

        # Set the meta data
        taskflow_instance['meta'] = taskflow.get('meta', {})
        # Mark the taskflow as being used to termination
        taskflow_instance['terminate'] = True

        taskflow_instance.terminate()

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.ADMIN)
    @describeRoute(
        Description('Start the taskflow ')
        .param(
            'id',
            'The id of task',
            required=True, paramType='path')
        .param(
            'body',
            'The input to the taskflow',
            required=False, paramType='body')
    )
    def start(self, taskflow, params):
        user = self.getCurrentUser()

        try:
            params = getBodyJson()
        except RestException:
            params = {}

        constructor = load_class(taskflow['taskFlowClass'])
        token = ModelImporter.model('token').createToken(user=user, days=7)

        workflow = constructor(
            id=str(taskflow['_id']),
            girder_token=token['_id'],
            girder_api_url=cumulus.config.girder.baseUrl)

        workflow.start(**params)

    @access.user(scope=TokenScope.DATA_WRITE)
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

        status = self._model.status(user, taskflow)
        if status == TaskFlowState.RUNNING:
            raise RestException('Taskflow is running', 400)

        constructor = load_class(taskflow['taskFlowClass'])
        token = ModelImporter.model('token').createToken(user=user, days=7)

        if taskflow['status'] != TaskFlowState.DELETING:

            taskflow['status'] = TaskFlowState.DELETING
            self._model.save(taskflow)

            workflow = constructor(
                id=str(taskflow['_id']),
                girder_token=token['_id'],
                girder_api_url=cumulus.config.girder.baseUrl)

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

    @access.user(scope=TokenScope.DATA_READ)
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

        cursor = ModelImporter.model('task', 'taskflow').find_by_taskflow_id(
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

    @access.user(scope=TokenScope.DATA_WRITE)
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

        task = ModelImporter.model('task', 'taskflow').create(user, taskflow, task)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/tasks/%s' % task['_id']

        return task

    @access.user(scope=TokenScope.DATA_WRITE)
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

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.ADMIN)
    @describeRoute(None)
    def delete_finished(self, taskflow, params):
        self._model.delete(taskflow)

    @access.user(scope=TokenScope.DATA_READ)
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.READ)
    @describeRoute(
        Description('Get log entries for task')
        .param(
            'id',
            'The task to get log entries for.', paramType='path')
        .param(
            'offset',
            'A offset in to the log.', required=False,
            paramType='query')
    )
    def get_log(self, taskflow, params):
        offset = 0
        if 'offset' in params:
            offset = int(params['offset'])

        return {'log': taskflow['log'][offset:]}

    addModel('ShareProperties', {
        'id': 'ShareProperties',
        'properties': {
            'users': {
                'type': 'array',
                'items': {
                    'type': 'string'
                },
                'description': 'array of user id\'s'
            },
            'groups': {
                'type': 'array',
                'items': {
                    'type': 'string'
                },
                'description': 'array of group id\'s'
            }
        }
    }, 'taskflows')

    @access.user(scope=TokenScope.DATA_READ)
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.ADMIN)
    @describeRoute(
        Description('Get access list for a taskflow')
        .param('id', 'The id of taskflow',
               required=True, paramType='path')
    )
    def get_access(self, taskflow, params):
        return taskflow.get('access', {'groups': [], 'users': []})

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.ADMIN)
    @describeRoute(
        Description('Set access list for a taskflow given a list of user and \
                    group ids')
        .param('id', 'The id of taskflow',
               required=True, paramType='path')
        .param('body', 'Users and group ID\'s to be authorized.',
               dataType='ShareProperties', required=True, paramType='body')
    )
    def set_access(self, taskflow, params):
        user = self.getCurrentUser()
        body = getBodyJson()
        return self._model.set_access(user, taskflow,
                                      body['users'], body['groups'], True)

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.ADMIN)
    @describeRoute(
        Description('Append access to a taskflow and its tasks')
        .param('id', 'The id of taskflow',
               required=True, paramType='path')
        .param('body', 'Users and group ID\'s to share taskflow with.',
               dataType='ShareProperties', required=True, paramType='body')
    )
    def patch_access(self, taskflow, params):
        user = self.getCurrentUser()
        body = getBodyJson()
        return self._model.patch_access(user, taskflow,
                                        body['users'], body['groups'])

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.ADMIN)
    @describeRoute(
        Description('Revoke access to a taskflow and its tasks')
        .param('id', 'The id of taskflow',
               required=True, paramType='path')
        .param('body', 'Users and group ID\'s to be unauthorized.',
               dataType='ShareProperties', required=True, paramType='body')
    )
    def revoke_access(self, taskflow, params):
        user = self.getCurrentUser()
        body = getBodyJson()
        return self._model.revoke_access(user, taskflow,
                                         body['users'], body['groups'])
