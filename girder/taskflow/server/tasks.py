#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2015 Kitware Inc.
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

from girder.api.rest import RestException, getBodyJson
from girder.api import access
from girder.api.describe import Description
from girder.api.docs import addModel
from girder.constants import AccessType
from .base import BaseResource
import sys
from cumulus.task import runner
from bson.objectid import ObjectId

class Tasks(BaseResource):

    def __init__(self):
        super(Tasks, self).__init__()
        self.resourceName = 'tasks'
        self.route('POST', (), self.create)
        self.route('PATCH', (':id',), self.update)
        self.route('GET', (':id', 'status'), self.status)
        self.route('GET', (':id',), self.get)
        self.route('GET', (), self.find)
        self.route('POST', (':id', 'log'), self.log)
        self.route('PUT', (':id', 'terminate'), self.terminate)
        self.route('DELETE', (':id',), self.delete)
        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('task', 'taskflow')

    def _clean(self, task):
        if 'access' in task:
            del task['access']

        return task

    def _check_status(self, request):
        if request.status_code != 200:
            print >> sys.stderr, request.content
            request.raise_for_status()

    @access.user
    def create(self, params):
        user = self.getCurrentUser()

        try:
            task = getBodyJson()
        except RestException:
            task = {}

        if 'taskFlowId' in task:
            task['taskFlowId']= ObjectId(task['taskFlowId'])
            # If a taskFlowId was provided then add this task to that flow.
            model = self.model('taskflow', 'taskflow')
            taskflow = model.load(task['taskFlowId'])
            taskflow.setdefault('tasks', []).append(task['_id'])
            model.save(taskflow)

        task['status'] = 'created'
        task = self._model.create(user, task)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/tasks/%s' % task['_id']

        return self._clean(task)

    addModel('TaskIdParam', {
        'id': 'TaskIdParam',
        'properties': {
            'taskSpecId': {
                'type': 'string'
            }
        }
    }, 'tasks')

    create.description = (
        Description('Create task from a spec file id')
        .param(
            'body',
            'The JSON parameters',
            required=True, paramType='body', dataType='TaskIdParam'))

    @access.user
    def update(self, id, params):
        user = self.getCurrentUser()

        body = cherrypy.request.body.read()

        if not body:
            raise RestException('A body must be provided', code=400)

        updates = json.loads(body)

        task = self._model.load(id, user=user, level=AccessType.WRITE)
        if not task:
            raise RestException('Task not found.', code=404)

        task.update(updates)
        self._model.update_task(user, task)

        return self._clean(task)

    update.description = (
        Description('Update the task')
        .param(
            'id',
            'The id of task',
            required=True, paramType='path')
        .param(
            'updates',
            'The properties to update',
            required=False, paramType='body'))

    @access.user
    def status(self, id, params):
        user = self.getCurrentUser()

        task = self._model.load(id, user=user, level=AccessType.READ)
        if not task:
            raise RestException('Task not found.', code=404)

        return {'status': task['status']}

    status.description = (
        Description('Get the task status')
        .param(
            'id',
            'The id of task',
            required=True, paramType='path'))

    @access.user
    def get(self, id, params):
        user = self.getCurrentUser()

        task = self._model.load(id, user=user, level=AccessType.READ)

        if not task:
            raise RestException('Task not found.', code=404)

        return self._clean(task)

    get.description = (
        Description('Get the task ')
        .param(
            'id',
            'The id of task',
            required=True, paramType='path'))

    @access.user
    def log(self, id, params):
        user = self.getCurrentUser()

        task = self._model.load(id, user=user, level=AccessType.WRITE)

        if not task:
            raise RestException('Task not found.', code=404)

        body = cherrypy.request.body.read()

        if not body:
            raise RestException('Log entry must be provided', code=400)

        task['log'].append(json.loads(body))
        self._model.update_task(user, task)

    log.description = None

    @access.user
    def terminate(self, id, params):
        user = self.getCurrentUser()

        task = self._model.load(id, user=user, level=AccessType.WRITE)

        if not task:
            raise RestException('Task not found.', code=404)


    terminate.description = (
        Description('Terminate the task ')
        .param(
            'id',
            'The id of task',
            required=True, paramType='path'))

    @access.user
    def delete(self, id, params):
        user = self.getCurrentUser()

        task = self._model.load(id, user=user, level=AccessType.WRITE)

    delete.description = (
        Description('Delete the task ')
        .param(
            'id',
            'The id of task',
            required=True, paramType='path'))


    @access.user
    def find(self, params):
        user = self.getCurrentUser()

        self.requireParams(['celeryTaskId'], params)

        task = self._model.find_by_celery_task_id(user, params['celeryTaskId'])

        return self._clean(task)

