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

from girder.api.rest import RestException, loadmodel, filtermodel, getBodyJson,\
    getCurrentUser
from girder.api.rest import Resource
from girder.api import access
from girder.api.describe import Description, describeRoute
from girder.constants import AccessType, TokenScope
from girder.utility.model_importer import ModelImporter

class Tasks(Resource):

    def __init__(self):
        super(Tasks, self).__init__()
        self.resourceName = 'tasks'
        self.route('PATCH', (':id',), self.update)
        self.route('GET', (':id', 'status'), self.status)
        self.route('GET', (':id',), self.get)
        self.route('POST', (':id', 'log'), self.log)
        self.route('GET', (':id', 'log'), self.get_log)

        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = ModelImporter.model('task', 'taskflow')

    @access.user(scope=TokenScope.DATA_WRITE)
    @filtermodel(model='task', plugin='taskflow')
    @loadmodel(model='task', plugin='taskflow', level=AccessType.WRITE)
    @describeRoute(
        Description('Update the task')
        .param(
            'id',
            'The id of task',
            required=True, paramType='path')
        .param(
            'updates',
            'The properties to update',
            required=False, paramType='body', dataType='object')
    )
    def update(self, task, params):
        immutable = ['access', '_id', 'celeryTaskId', 'log']
        user = getCurrentUser()
        updates = getBodyJson()
        if not updates:
            raise RestException('A body must be provided', code=400)

        for p in updates:
            if p in immutable:
                raise RestException('\'%s\' is an immutable property' % p, 400)

        status = updates.get('status')

        return self._model.update_task(user, task, status=status)

    @access.user(scope=TokenScope.DATA_READ)
    @loadmodel(model='task', plugin='taskflow', level=AccessType.READ)
    @describeRoute(
        Description('Get the task status')
        .param('id', 'The id of task', required=True, paramType='path')
    )
    def status(self, task, params):
        return {'status': task['status']}

    @access.user(scope=TokenScope.DATA_READ)
    @filtermodel(model='task', plugin='taskflow')
    @loadmodel(model='task', plugin='taskflow', level=AccessType.READ)
    @describeRoute(
        Description('Get the task ')
        .param(
            'id',
            'The id of task',
            required=True, paramType='path')
    )
    def get(self, task, params):
        return task

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='task', plugin='taskflow', level=AccessType.WRITE)
    @describeRoute(None)
    def log(self, task, params):
        body = cherrypy.request.body.read().decode('utf8')

        if not body:
            raise RestException('Log entry must be provided', code=400)

        self._model.append_to_log(task, json.loads(body))

    @access.user(scope=TokenScope.DATA_READ)
    @loadmodel(model='task', plugin='taskflow', level=AccessType.READ)
    @describeRoute(
        Description('Get log entries for task')
        .param('id', 'The task to get log entries for.', paramType='path')
        .param('offset', 'A offset in to the log.', required=False,
               paramType='query')
    )
    def get_log(self, task, params):
        offset = 0
        if 'offset' in params:
            offset = int(params['offset'])

        return {'log': task['log'][offset:]}
