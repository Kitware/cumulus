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
from girder.api.rest import Resource, RestException, getCurrentUser
from girder.api import access
from girder.constants import SettingKey

newt_base_url = 'https://newt.nersc.gov/newt'

class Newt(Resource):
    def __init__(self):
        super(Newt, self).__init__()
        self.resourceName = 'newt'

        self.route('PUT', ('authenticate', ':sessionId'), self.authenticate)
        self.route('GET', ('sessionId',), self.session_id)

    def _create_or_reuse_user(self, user_id, user_name, email, first_name,
                              last_name):

        # Try finding by user id
        query = {
            'newt.id': user_id
        }

        user = self.model('user').findOne(query)
        set_id = not user

        # Existing users using NEWT for the first time will not have an user id
        if not user:
            user = self.model('user').findOne({'email': email})

        dirty = False
        # Create the user if it's still not found
        if not user:
            policy = self.model('setting').get(SettingKey.REGISTRATION_POLICY)
            if policy != 'open':
                raise RestException(
                    'Registration on this instance is closed. Contact an '
                    'administrator to create an account for you.')

            user = self.model('user').createUser(
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
            user.setdefault('newt', []).append(
                {
                    'id': user_id
                })
            dirty = True

        if dirty:
            user = self.model('user').save(user)

        return user

    def send_cookies(self, user, sessionId):
        super(Newt, self).sendAuthTokenCookie(user)
        cookie = cherrypy.response.cookie
        cookie['newt_sessionid'] = sessionId

    @access.public
    def authenticate(self, sessionId, params):
        status_url = '%s/login' % newt_base_url
        cookies = dict(newt_sessionid=sessionId)

        r = requests.get(status_url, cookies=cookies)
        json_resp = r.json()

        # Check that we have a valid session id
        if not json_resp['auth']:
            raise RestException('Authentication failed.', code=403)

        # Now get the use information so we can lookup the Girder user
        username = json_resp['username']
        r = requests.get('%s/account/user/%s/persons' %
                         (newt_base_url, username), cookies=cookies)
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
                                          lastname)

        self.send_cookies(user, sessionId)

        return user

    authenticate.description = (
        Description('Authenticate with Girder using a NEWT session id.')
        .param('sessionId', 'The NEWT session id', paramType='path'))

    @access.user
    def session_id(self):
        user = getCurrentUser()

        return {
            'sessionId': user.get('newt', {}).get('sessionId')
        }

    session_id.description = (
        Description('Returns the NEWT session id for this user'))




