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
import paramiko
from jsonpath_rw import parse

from base_integration_test import BaseIntegrationTest
from girder_client import HttpError

class TradIntegrationTest(BaseIntegrationTest):

    def __init__(self, name, girder_url, girder_user, girder_password, user,
                  host):
        super(TradIntegrationTest, self).__init__(name, girder_url, girder_user,
                                                  girder_password)
        self._user = user
        self._host = host

    def tearDown(self):
        super(TradIntegrationTest, self).tearDown()
        try:
            url = 'clusters/%s' % self._cluster_id
            self._client.delete(url)
        except Exception:
            traceback.print_exc()

    def create_cluster(self):
        body = {
            'config': {
                'ssh': {
                    'user': self._user
                },
                'host': self._host
            },
            'name': 'TradIntegrationTest',
            'type': 'trad'
        }

        r = self._client.post('clusters', data=json.dumps(body))
        self._cluster_id = r['_id']

        sleeps = 0
        while True:
            time.sleep(1)
            r = self._client.get('clusters/%s/status' % self._cluster_id)

            if r['status'] == 'created':
                break

            if sleeps > 9:
                self.fail('Cluster never moved into created state')

            sleeps += 1

        r = self._client.get('clusters/%s' % self._cluster_id)

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.load_system_host_keys()
        client.connect(self._host, username=self._user)
        key = parse('config.ssh.publicKey').find(r)

        if not key:
            self.fail('No public key generated')

        key = key[0].value

        _, stdout, stderr = client.exec_command('echo "%s" >> ~/.ssh/authorized_keys' % key)
        self.assertFalse(stdout.read())
        self.assertFalse(stderr.read())

        # Now test the connection
        r = self._client.put('clusters/%s/start' % self._cluster_id)
        sleeps = 0
        while True:
            time.sleep(1)
            r = self._client.get('clusters/%s/status' % self._cluster_id)

            if r['status'] == 'running':
                break

            if sleeps > 9:
                self.fail('Cluster never moved into running state')
            sleeps += 1

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

if __name__ == '__main__':

    parser = argparse.ArgumentParser(parents=[BaseIntegrationTest.parser])
    parser.add_argument('-n', '--host', help='', required=True)
    parser.add_argument('-u', '--user', help='', required=True)

    args = parser.parse_args()

    suite = unittest.TestSuite()
    suite.addTest(TradIntegrationTest("test", args.girder_url, args.girder_user,
        args.girder_password, args.user, args.host))
    unittest.TextTestRunner().run(suite)
