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
import mock
import httmock
import json

from cumulus.tasks import cluster
from cumulus.testing import AssertCallsMixin

class TradClusterTestCase(AssertCallsMixin, unittest.TestCase):

    def setUp(self):
        self._expected_status = 'error'
        self._set_status_called  = False
        self._set_status_valid  = False

    @mock.patch('cumulus.tasks.cluster.get_connection')
    def test_connection(self, get_connection):

        def valid(self):
            return True

        cluster_id = 'dummy_id'
        cluster_model = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'conn': {
                    'user': 'bob'
                },
                'host': 'myhost'
            },
            '_id': cluster_id
        }

        self._set_call_value_index = 0
        def _set_status(url, request):
            expected = {'status': self._expected_status}
            self._set_status_called = True
            self._set_status_valid = json.loads(request.body.decode('utf8')) == expected
            self._set_status_request = request.body.decode('utf8')

            return httmock.response(200, None, {}, request=request)

        status_update_url = '/api/v1/clusters/%s' % cluster_id
        set_status = httmock.urlmatch(
            path=r'^%s$' % status_update_url, method='PATCH')(_set_status)

        def _log(url, request):
            return httmock.response(200, None, {}, request=request)

        log_url = '/api/v1/clusters/%s/log' % cluster_id
        log = httmock.urlmatch(
            path=r'^%s$' % log_url, method='POST')(_log)

        with httmock.HTTMock(set_status, log):
            cluster.test_connection(cluster_model, **{'girder_token': 's'})

        self.assertTrue(self._set_status_called, 'Set status endpoint not called')
        self.assertTrue(self._set_status_valid,
                        'Set status endpoint called with incorrect content: %s'
                            % self._set_status_request)

        # Mock our conn calls and try again
        def _get_cluster(url, request):
            content =   {
                "_id": "55ef53bff657104278e8b185",
                "config": {
                    "host": "ulmus",
                    "conn": {
                        "publicKey": "conn-rsa dummy",
                        'passphrase': 'dummy',
                        "user": "test"
                    }
                }
            }

            content = json.dumps(content).encode('utf8')
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }
            return httmock.response(200, content, headers, request=request)

        cluster_url = '/api/v1/clusters/%s' % cluster_id
        get_cluster = httmock.urlmatch(
            path=r'^%s$' % cluster_url, method='GET')(_get_cluster)


        conn = get_connection.return_value.__enter__.return_value
        conn.execute.return_value = ['/usr/bin/qsub']
        self._expected_status = 'running'
        with httmock.HTTMock(set_status, get_cluster):
            cluster.test_connection(cluster_model, **{'girder_token': 's', 'log_write_url': 'http://localhost/log'})

        self.assertTrue(self._set_status_called, 'Set status endpoint not called')
        self.assertTrue(self._set_status_valid,
                        'Set status endpoint called in incorrect content: %s'
                            % self._set_status_request)


