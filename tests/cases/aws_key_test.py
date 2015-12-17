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

import unittest
import httmock
import os
import json
from jsonpath_rw import parse
import mock

import cumulus
try:
    from cumulus.aws.ec2.tasks import key
except:
    import traceback
    traceback.print_exc()

class KeyTestCase(unittest.TestCase):

    def setUp(self):
        self._update = False
        self._errorMessage = None
        self._expected_status = 'creating'

    @mock.patch('cumulus.aws.ec2.tasks.key.get_easy_ec2')
    def test_key_generate(self, get_simple_ec2):
        ec2 = get_simple_ec2.return_value
        ec2.create_keypair.side_effect = Exception('some error')

        profile = {
            '_id': '55c3a698f6571011a48f6817',
            'userId': '55c3a698f6571011a48f6818',
            'name': 'profile'
        }

        key_path = os.path.join(cumulus.config.ssh.keyStore, profile['_id'])
        try:
            os.remove(key_path)
        except OSError:
            pass

        def _update(url, request):
            request_body = json.loads(request.body)
            status = parse('status').find(request_body)
            if status:
                status = status[0].value

            self._update = status == self._expected_status

            if status == 'error':
                self._errorMessage = request_body['errorMessage']

            return httmock.response(200, None, {}, request=request)

        update_url = '/api/v1/user/%s/aws/profiles/%s' % (profile['userId'],
                                                         profile['_id'])
        update = httmock.urlmatch(
            path=r'^%s$' % update_url, method='PATCH')(_update)

        self._expected_status = 'error'
        with httmock.HTTMock(update):
            key.generate_key_pair(profile, 'girder-token')

        self.assertTrue(self._update, 'Update was not called')
        self.assertTrue(self._errorMessage, 'No errorMessage set')

        # Now mock out EC2 and check for success
        self._update = False
        self._expected_status = 'available'
        ec2.create_keypair.side_effect = None
        with httmock.HTTMock(update):
            key.generate_key_pair(profile, 'girder-token')

