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
import time
import traceback

from girder.api.rest import RestException, getBodyJson, loadmodel,\
    getCurrentUser, getApiUrl
from girder.api import access
from girder.api.describe import Description
from girder.api.docs import addModel
from girder.constants import AccessType
from .base import BaseResource
import requests
import sys
from cumulus.task import runner
from bson.objectid import ObjectId

from .utility import load_class


class TaskFlows(BaseResource):

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
        self.route('GET', (':id','tasks'), self.tasks)
        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('taskflow', 'taskflow')

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
        taskflow = getBodyJson()

        self.requireParams(['taskFlowClass'], taskflow)

        # Check that we can load the class
        try:
            load_class(taskflow['taskFlowClass'])
        except:
            raise RestException(
                    'Unable to taskflow task: %s' % taskflow['taskFlowClass'])
        taskflow = self._model.create(user, taskflow)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/taskflows/%s' % taskflow['_id']

        return self._clean(taskflow)

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

        if 'status' in updates:
            task['status'] = updates['status']

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

    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.READ)
    def status(self, taskflow, params):
        user = self.getCurrentUser()
        tasks = self.model('task', 'taskflow').find_by_taskflow_id(
            user, taskflow['_id'])

        task_status = [t['status'] for t in tasks]
        task_status = set(task_status)

        status = 'created'
        if len(task_status) ==  1:
            status = task_status.pop()
        elif 'error' in task_status:
            status = 'error'
        elif 'running' in task_status or \
             ('complete'in task_status and 'created' in task_status):
            status = 'running'

        return {'status': status}

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
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.ADMIN)
    def start(self, taskflow, params):
        user = self.getCurrentUser()


        constructor = load_class(taskflow['taskFlowClass'])
        token = self.model('token').createToken(user=user, days=7)

        workflow = constructor(
            id=str(taskflow['_id']),
            girder_token=token['_id'],
            girder_api_url=getApiUrl())

        workflow.start()

    start.description = (
        Description('Start the taskflow ')
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
    def tasks(self, id, params):
        user = getCurrentUser()
        cursor = self.model('task', 'taskflow').find_by_taskflow_id(
                                                    user, ObjectId(id))

        return [self._clean(task) for task in cursor]

    @access.user
    @loadmodel(model='taskflow', plugin='taskflow', level=AccessType.READ)
    def create_task(self, taskflow, params):
        user = getCurrentUser()
        task = getBodyJson()

        self.requireParams(['celeryTaskId'], task)

        task = self.model('task', 'taskflow').create(user, taskflow, task)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/tasks/%s' % task['_id']

        return self._clean(task)

