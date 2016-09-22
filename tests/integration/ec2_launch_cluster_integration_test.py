#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2016 Kitware Inc.
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


class AnsibleIntegrationTest(BaseIntegrationTest):

    def __init__(self, name, girder_url, girder_user, girder_password, aws_access_key_id,
                  aws_secret_access_key):
        super(AnsibleIntegrationTest, self).__init__(name, girder_url, girder_user,
                                                  girder_password)
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._cluster_id = None
        self._profile_id = None

    def tearDown(self):
        super(AnsibleIntegrationTest, self).tearDown()
        if self._cluster_id:
            try:
                url = 'clusters/%s' % self._cluster_id
                self._client.delete(url)
            except Exception:
                traceback.print_exc()

        if self._profile_id:
            try:
                url = 'user/%s/aws/profiles/%s' % (self._user_id,
                                                   self._profile_id)
                self._client.delete(url)
            except Exception:
                traceback.print_exc()

    def _wait_for_status(self, status_url, status, timeout=10):

        start = time.time();
        while True:
            time.sleep(1)
            r = self._client.get(status_url)

            if r['status'] == status:
                break

            if time.time() - start > timeout:
                self.fail('Resource never moved into the "%s" state, current '
                          'state is "%s"' % (status, r['status']))


    def create_profile(self):
        user = self._client.get('user/me')
        self._user_id = user['_id']

        profile_url = 'user/%s/aws/profiles' % user['_id']
        body = {
            'accessKeyId': self._aws_access_key_id,
            'availabilityZone': 'us-west-2a',
            'name': 'testTest',
            'regionName': 'us-west-2',
            'cloudProvider': 'ec2',
            'secretAccessKey': self._aws_secret_access_key
        }

        r = self._client.post(profile_url, data=json.dumps(body))
        self._profile_id = r['_id']

        profile_status_url \
            = 'user/%s/aws/profiles/%s/status' % (user['_id'], self._profile_id)
        self._wait_for_status(profile_status_url, 'available')


    def create_cluster(self, cluster_type='ansible'):
        body = {
            'config': {
                'launch': {
                    'spec': 'ec2',
                    'params': {
                        'master_instance_type': 't2.nano',
                        'master_instance_ami': 'ami-03de3c63',
                        'node_instance_count': 1,
                        'node_instance_type': 't2.nano',
                        'node_instance_ami': 'ami-03de3c63',
                        'terminate_wait_timeout': 240
                    }
                }
            },
            'profileId': self._profile_id,
            'name': 'AnsibleIntegrationTest',
            'type': cluster_type
        }

        r = self._client.post('clusters', data=json.dumps(body))
        self._cluster_id = r['_id']

    def launch_cluster(self):
        cluster_url = 'clusters/%s/launch' % self._cluster_id
        self._client.put(cluster_url)

        status_url = 'clusters/%s/status' % self._cluster_id
        self._wait_for_status(status_url, 'running', timeout=600)

    def provision_cluster(self):
        cluster_url = 'clusters/%s/provision' % self._cluster_id
        self._client.put(cluster_url, data=json.dumps({
            'spec': 'gridengine/site',
            'ssh': {
                'user': 'ubuntu'
            }
        }))

        status_url = 'clusters/%s/status' % self._cluster_id
        self._wait_for_status(status_url, 'running', timeout=600)

    def terminate_cluster(self):
        cluster_url = 'clusters/%s/terminate' % self._cluster_id
        self._client.put(cluster_url)

        status_url = 'clusters/%s/status' % self._cluster_id
        self._wait_for_status(status_url, 'terminated', timeout=300)


    def test(self):
        try:
            self.create_profile()
            self.create_cluster()
            self.launch_cluster()
            self.terminate_cluster()
        except HttpError as error:
            self.fail(error.responseText)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(parents=[BaseIntegrationTest.parser])
    parser.add_argument('-i', '--aws_access_key_id', help='', required=True)
    parser.add_argument('-a', '--aws_secret_access_key', help='', required=True)

    args = parser.parse_args()

    suite = unittest.TestSuite()
    suite.addTest(AnsibleIntegrationTest('test', args.girder_url, args.girder_user,
        args.girder_password, args.aws_access_key_id, args.aws_secret_access_key))
    unittest.TextTestRunner().run(suite)
