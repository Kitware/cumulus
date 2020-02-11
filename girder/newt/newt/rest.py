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
from girder.api.rest import Resource, RestException, getCurrentUser, getBodyJson
from girder.api.rest import loadmodel
from girder.api import access
from girder.settings import SettingKey
from girder.constants import AssetstoreType, AccessType, TokenScope
from girder.api.docs import addModel
from girder.models.setting import Setting
from girder.models.user import User
from girder.models.item import Item
from girder.models.file import File
from girder.models.assetstore import Assetstore


from .constants import NEWT_BASE_URL, PluginSettings


class Newt(Resource):
    def __init__(self):
        super(Newt, self).__init__()
        self.resourceName = 'newt'

        self.route('PUT', ('authenticate', ':sessionId'), self.authenticate)
        self.route('GET', ('sessionId',), self.session_id)

    def _create_or_reuse_user(self, user_id, user_name, email, first_name,
                              last_name, session_id):

        # Try finding by user id
        query = {
            'newt.id': user_id
        }

        user = User().findOne(query)
        set_id = not user

        # Existing users using NEWT for the first time will not have an user id
        if not user:
            user = User().findOne({'email': email})

        # Create the user if it's still not found
        if not user:
            policy = Setting().get(SettingKey.REGISTRATION_POLICY)
            if policy == 'closed':
                ignore = Setting().get(PluginSettings.IGNORE_REGISTRATION_POLICY)
                if not ignore:
                    raise RestException(
                        'Registration on this instance is closed. Contact an '
                        'administrator to create an account for you.')

            user = User().createUser(
                login=user_name, password=None, firstName=first_name,
                lastName=last_name, email=email)
        else:
            # Update user data from NEWT
            if email != user['email']:
                user['email'] = email
                dirty = True
            # Don't set names to empty string
            if first_name != user['firstName'] and first_name:
                user['firstName'] = first_name
                dirty = True
            if last_name != user['lastName'] and last_name:
                user['lastName'] = last_name
                dirty = True

        if set_id:
            user.setdefault('newt', {})['id'] = user_id

        user.setdefault('newt', {})['sessionId'] = session_id

        user = User().save(user)

        return user

    def send_cookies(self, user, session_id):
        super(Newt, self).sendAuthTokenCookie(user)
        cookie = cherrypy.response.cookie
        cookie['newt_sessionid'] = session_id

    @access.public
    def authenticate(self, sessionId, params):
        status_url = '%s/login' % NEWT_BASE_URL
        cookies = dict(newt_sessionid=sessionId)

        r = requests.get(status_url, cookies=cookies)
        json_resp = r.json()

        # Check that we have a valid session id
        if not json_resp['auth']:
            raise RestException('Authentication failed.', code=403)

        # Now get the use information so we can lookup the Girder user
        username = json_resp['username']
        r = requests.get('%s/account/user/%s/persons' %
                         (NEWT_BASE_URL, username), cookies=cookies)
        json_resp = r.json()

        if len(json_resp['items']) != 1:
            raise RestException('Authentication failed.', code=403)

        user_info = json_resp['items'][0]
        user_id = user_info['user_id']
        username = user_info['uname']
        email = user_info['email']
        firstname = user_info['firstname']
        lastname = user_info['lastname']

        user = self._create_or_reuse_user(user_id, username, email, firstname,
                                          lastname, sessionId)

        self.send_cookies(user, sessionId)

        return user

    authenticate.description = (
        Description('Authenticate with Girder using a NEWT session id.')
        .param('sessionId', 'The NEWT session id', paramType='path'))

    @access.user(scope=TokenScope.DATA_READ)
    def session_id(self, params):
        user = getCurrentUser()

        user = User().load(user['_id'], fields=['newt'], force=True)

        return {
            'sessionId': user.get('newt', {}).get('sessionId')
        }

    session_id.description = (
        Description('Returns the NEWT session id for this user'))

def create_assetstore(params):
    """Create a new NEWT assetstore."""

    return Assetstore().save({
        'type': AssetstoreType.NEWT,
        'name': params.get('name'),
        'newt': {
            'machine': params.get('machine'),
            'baseUrl': params.get('baseUrl', NEWT_BASE_URL)
        }
    })

class NewtAssetstore(Resource):
    def __init__(self):
        super(NewtAssetstore, self).__init__()
        self.resourceName = 'newt_assetstores'

        self.route('POST', (), self.create)
        self.route('POST', (':id', 'files'), self.create_file)

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
        item = Item().load(id=item_id, user=user,
                           level=AccessType.WRITE, exc=True)

        file = File().createFile(
                        name=name, creator=user, item=item, reuseExisting=True,
                        assetstore=assetstore, mimeType=mime_type, size=size)

        file['path'] = path
        file['imported'] = True
        File().save(file)

        return File().filter(file)

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
        }, 'newt')

    create_file.description = (
         Description('Create a new file in this assetstore.')
        .param('id', 'The the assetstore to create the file in', required=True,
               paramType='path')
        .param('body', 'The parameter to create the file with.', required=True,
               paramType='body', dataType='CreateFileParams'))

    @access.user(scope=TokenScope.DATA_WRITE)
    def create(self, params):
        params = getBodyJson()
        self.requireParams(('name', 'machine'), params)

        return create_assetstore(params)

    addModel('CreateAssetstoreParams', {
        'id': 'CreateAssetstoreParams',
        'required': ['name', 'itemId', 'size', 'path'],
        'properties': {
            'name': {'type': 'string',
                     'description': '.'},
            'machine':  {'type': 'string',
                          'description': 'The host where files are stored.'}
            }
        }, 'newt')

    create.description = (
     Description('Create a new NEWT assetstore.')
    .param('body', 'The parameter to create the assetstore', required=True,
               paramType='body', dataType='CreateAssetstoreParams'))
