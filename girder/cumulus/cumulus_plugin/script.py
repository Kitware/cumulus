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
from girder.api import access
from girder.api.describe import Description
from girder.api.docs import addModel
from girder.constants import AccessType, TokenScope
from girder.api.rest import RestException
from girder.utility.model_importer import ModelImporter
from .base import BaseResource


class Script(BaseResource):

    def __init__(self):
        super(Script, self).__init__()
        self.resourceName = 'scripts'
        self.route('POST', (), self.create)
        self.route('GET', (':id',), self.get)
        self.route('PATCH', (':id', 'import'), self.import_script)
        self.route('PUT', (':id', 'access'), self.update_access)
        self.route('DELETE', (':id',), self.delete)
        self._model = ModelImporter.model('script', 'cumulus')

    def _clean(self, config):
        del config['access']

        return config

    @access.user(scope=TokenScope.DATA_WRITE)
    def import_script(self, id, params):
        user = self.getCurrentUser()
        lines = cherrypy.request.body.read().decode('utf8').splitlines()

        script = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not script:
            raise RestException('Script doesn\'t exist', code=404)

        script['commands'] = lines
        self._model.save(script)

        return self._clean(script)

    import_script.description = (
        Description('Import script')
        .param(
            'id',
            'The script to upload lines to',
            required=True, paramType='path')
        .param(
            'body',
            'The contents of the script',
            required=True, paramType='body')
        .consumes('text/plain'))

    @access.user(scope=TokenScope.DATA_WRITE)
    def create(self, params):
        user = self.getCurrentUser()

        script = json.loads(cherrypy.request.body.read().decode('utf8'))

        if 'name' not in script:
            raise RestException('Script name is required', code=400)

        script = self._model.create(user, script)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/scripts/%s' % script['_id']

        return self._clean(script)

    addModel('Script', {
        'id': 'Script',
        'required': 'global',
        'properties': {
            'name': {
                'type': 'string'
            },
            'commands': {
                'type': 'array',
                'items': {
                    'type': 'string'
                }
            }
        }
    }, 'scripts')

    create.description = (
        Description('Create script')
        .param(
            'body',
            'The JSON contain script parameters',
            required=True, paramType='body', dataType='Script'))

    @access.user(scope=TokenScope.DATA_READ)
    def get(self, id, params):
        user = self.getCurrentUser()

        script = self._model.load(id, user=user, level=AccessType.READ)

        if not script:
            raise RestException('Script not found', code=404)

        return self._clean(script)

    get.description = (
        Description('Get script')
        .param(
            'id',
            'The id of the script to get',
            required=True, paramType='path'))

    @access.user(scope=TokenScope.DATA_WRITE)
    def delete(self, id, params):
        user = self.getCurrentUser()
        script = self._model.load(id, user=user, level=AccessType.ADMIN)

        self._model.remove(script)

    delete.description = (
        Description('Delete a script')
        .param(
            'id',
            'The script id.', paramType='path', required=True))

    @access.user(scope=TokenScope.DATA_WRITE)
    def update_access(self, id, params):
        user = self.getCurrentUser()

        body = cherrypy.request.body.read()

        if not body:
            raise RestException('No message body provided', code=400)

        body = json.loads(body.decode('utf8'))

        script = self._model.load(id, user=user, level=AccessType.WRITE)
        if not script:
            raise RestException('Script not found.', code=404)

        script = self._model.setAccessList(script, body, save=True)

        return script

    update_access.description = (
        Description('Update script access')
        .param(
            'id',
            'The script to update',
            required=True, paramType='path')
        .param(
            'body',
            'The fields to update',
            required=True, paramType='body')
        .consumes('application/json'))
