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

import argparse
import unittest
import json
import time
import traceback
from requests import Session

from base_integration_test import BaseIntegrationTest
from girder_client import GirderClient, HttpError


class NewtIntegrationTest(BaseIntegrationTest):

    def __init__(self, name, girder_url, girder_user, girder_password, machine,
                 job_timeout=60*5):
        super(NewtIntegrationTest, self).__init__(name, girder_url, girder_user,
                                                  girder_password, job_timeout)
        self._cluster_id = None
        self._machine = machine

    def setUp(self):

        # First authenticate with NEWT
        self._session = Session()
        r = self._session.post('https://newt.nersc.gov/newt/auth',
                               {
                                    'username': self._girder_user,
                                    'password': self._girder_password})

        self.assertEqual(r.status_code, 200)
        print r.json()
        self._newt_session_id = r.json()['newt_sessionid']

        # Now authenticate with Girder using the session id
        url = '%s/api/v1/newt/authenticate/%s' % (self._girder_url, self._newt_session_id)
        r = self._session.put(url)
        self.assertEqual(r.status_code, 200)

        url = '%s/api/v1/newt/authenticate/%s' % (self._girder_url, self._newt_session_id)
        r = self._session.put(url)
        self.assertEqual(r.status_code, 200)

        url = '%s/api/v1' % self._girder_url
        self._client = GirderClient(apiUrl=url)
        self._client.token = self._session.cookies['girderToken']

        user = self._client.get('user/me')
        self._user_id = user['_id']
        r = self._client.listFolder(self._user_id, 'user', name='Private')
        r = list(r)
        self.assertEqual(len(r), 1)
        self._private_folder_id = r[0]['_id']

    def tearDown(self):
        super(NewtIntegrationTest, self).tearDown()
        if self._cluster_id:
            try:
                url = 'clusters/%s' % self._cluster_id
                self._client.delete(url)
            except Exception:
                traceback.print_exc()

    def create_cluster(self):
        body = {
            'config': {
                'host': self._machine
            },
            'name': 'NewtIntegrationTest',
            'type': 'newt'
        }

        r = self._client.post('clusters', data=json.dumps(body))
        self._cluster_id = r['_id']

        # Now test the connection
        r = self._client.put('clusters/%s/start' % self._cluster_id)
        sleeps = 0
        while True:
            time.sleep(1)
            r = self._client.get('clusters/%s/status' % self._cluster_id)

            if r['status'] == 'running':
                break
            elif r['status'] == 'error':
                r = self._client.get('clusters/%s/log' % self._cluster_id)
                self.fail(str(r))

            if sleeps > 9:
                self.fail('Cluster never moved into running state')
            sleeps += 1

    def assert_output(self):
        r = self._client.listItem(self._output_folder_id)
        self.assertEqual(len(r), 4)

        stdout_item = None
        for i in r:
            if i['name'].startswith('CumulusIntegrationTestJob-%s.o' % self._job_id):
                stdout_item = i
                break

        self.assertIsNotNone(stdout_item)
        r = self._client.get('item/%s/files' % i['_id'])
        self.assertEqual(len(r), 1)

        url =   '%s/api/v1/file/%s/download' % (self._girder_url, r[0]['_id'])
        r = self._session.get(url)
        self.assertEqual(r.content, self._data)


    def test(self):
        try:
            self.create_cluster()
            self.create_script()
            self.create_input()
            self.create_output_folder()
            self.create_job()
            self.submit_job(timeout=self._job_timeout)
            self.assert_output()
        except HttpError as error:
            self.fail(error.responseText)

NewtIntegrationTest.parser = argparse.ArgumentParser(parents=[BaseIntegrationTest.parser])
NewtIntegrationTest.parser.add_argument('-m', '--machine', help='', required=True)

if __name__ == '__main__':
    args = NewtIntegrationTest.parser.parse_args()

    suite = unittest.TestSuite()
    suite.addTest(NewtIntegrationTest("test", args.girder_url, args.girder_user,
        args.girder_password, args.machine))
    unittest.TextTestRunner().run(suite)
