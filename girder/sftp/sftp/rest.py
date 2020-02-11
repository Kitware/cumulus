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

import requests
import cherrypy

from girder.api.describe import Description
from girder.api.rest import Resource, RestException, getBodyJson, loadmodel
from girder.api import access
from girder.settings import SettingKey
from girder.constants import AssetstoreType, AccessType, TokenScope
from girder.api.docs import addModel
from girder.utility.model_importer import ModelImporter

class SftpAssetstoreResource(Resource):
    def __init__(self):
        super(SftpAssetstoreResource, self).__init__()
        self.resourceName = 'sftp_assetstores'

        self.route('POST', (), self.create_assetstore)
        self.route('POST', (':id', 'files'), self.create_file)

    @access.user(scope=TokenScope.DATA_WRITE)
    def create_assetstore(self, params):
        """Create a new SFTP assetstore."""

        params = getBodyJson()
        self.requireParams(('name', 'host', 'user'), params)

        return ModelImporter.model('assetstore').save({
            'type': AssetstoreType.SFTP,
            'name': params.get('name'),
            'sftp': {
                'host': params.get('host'),
                'user': params.get('user'),
                'authKey': params.get('authKey')
            }
        })

    addModel('CreateAssetstoreParams', {
        'id': 'CreateAssetstoreParams',
        'required': ['name', 'itemId', 'size', 'path'],
        'properties': {
            'name': {'type': 'string',
                     'description': '.'},
            'host':  {'type': 'string',
                          'description': 'The host where files are stored.'},
            'user': {'type': 'number',
                       'description': 'The user to access the files.'},
            'authKey': {'type': 'string',
                       'description': 'A key that can be used to lookup authentication credentials.'}
            }
        }, 'sftp')

    create_assetstore.description = (
         Description('Create a new sftp assetstore.')
        .param('body', 'The parameter to create the assetstore', required=True,
               paramType='body', dataType='CreateAssetstoreParams'))


    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='assetstore')
    def create_file(self, assetstore, params):
        params = getBodyJson()
        self.requireParams(('name', 'itemId', 'size', 'path'), params)
        name = params['name']
        item_id = params['itemId']
        size = int(params['size'])
        path = params['path']
        user = self.getCurrentUser()

        mime_type = params.get('mimeType')
        item = ModelImporter.model('item').load(id=item_id, user=user,
                                      level=AccessType.WRITE, exc=True)

        file = ModelImporter.model('file').createFile(
                        name=name, creator=user, item=item, reuseExisting=True,
                        assetstore=assetstore, mimeType=mime_type, size=size)

        file['path'] = path
        file['imported'] = True
        ModelImporter.model('file').save(file)

        return ModelImporter.model('file').filter(file)

    addModel('CreateFileParams', {
        'id': 'CreateFileParams',
        'required': ['name', 'itemId', 'size', 'path'],
        'properties': {
            'name': {'type': 'string',
                     'description': 'The name of the file.'},
            'itemId':  {'type': 'string',
                          'description': 'The item to attach the file to.'},
            'size': {'type': 'number',
                       'description': 'The size of the file.'},
            'path': {'type': 'string',
                       'description': 'The full path to the file.'},
            'mimeType': {'type': 'string',
                       'description': 'The the mimeType of the file.'},

            }
        }, 'sftp')

    create_file.description = (
         Description('Create a new file in this assetstore.')
        .param('id', 'The the assetstore to create the file in', required=True,
               paramType='path')
        .param('body', 'The parameter to create the file with.', required=True,
               paramType='body', dataType='CreateFileParams'))





