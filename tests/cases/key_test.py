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

import cumulus
from cumulus.ssh.tasks import key

class KeyTestCase(unittest.TestCase):

    def setUp(self):
        self._update = False

    def test_key_generate(self):
        cluster = {
            '_id': '55c3a698f6571011a48f6817'
        }

        key_path = os.path.join(cumulus.config.ssh.keyStore, cluster['_id'])
        try:
            os.remove(key_path)
        except OSError:
            pass

        def _update(url, request):
            request_body = json.loads(request.body.decode('utf8'))
            passphrase = parse('config.ssh.passphrase').find(request_body)
            public_key = parse('config.ssh.publicKey').find(request_body)
            status = request_body['status'] == 'created'

            self._update = passphrase and public_key and status
            return httmock.response(200, None, {}, request=request)

        update_url = '/api/v1/clusters/%s' % cluster['_id']
        update = httmock.urlmatch(
            path=r'^%s$' % update_url, method='PATCH')(_update)

        with httmock.HTTMock(update):
            key.generate_key_pair(cluster)

        self.assertTrue(os.path.exists(key_path), 'Key was not created')
        os.remove(key_path)
        self.assertTrue(self._update, 'Update was not called')


