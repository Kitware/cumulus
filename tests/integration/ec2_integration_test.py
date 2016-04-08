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

from ansible_integration_test import AnsibleIntegrationTest
from girder_client import HttpError


class EC2IntegrationTest(AnsibleIntegrationTest):

    def __init__(self, name, girder_url, girder_user, girder_password, aws_access_key_id,
                  aws_secret_access_key):
        super(EC2IntegrationTest, self).__init__(
            name, girder_url, girder_user, girder_password, aws_access_key_id,
            aws_secret_access_key)

    def tearDown(self):
        super(EC2IntegrationTest, self).tearDown()

    def start_cluster(self):
        cluster_url = 'clusters/%s/start' % self._cluster_id
        body = {
            'spec': 'gridengine/site',
            'ssh': {
                'user': 'ubuntu'
            }
        }

        self._client.put(cluster_url, data=json.dumps(body))

        status_url = 'clusters/%s/status' % self._cluster_id
        self._wait_for_status(status_url, 'running', timeout=1200)

    def create_script(self):
        commands = [
            'sleep 15'
        ]
        super(AnsibleIntegrationTest, self).create_script(commands=commands)


    def create_job(self):
        super(AnsibleIntegrationTest, self).create_job(job_name='ec2test')


    def test(self):
        try:

            self.create_profile()
            self.create_cluster(cluster_type='ec2')
            self.start_cluster()
            self.create_script()
            self.create_input(folder_name='ec2_input')
            self.create_output_folder(folder_name='ec2_output')
            self.create_job()
            self.submit_job(timeout=300)
            self.terminate_cluster()
        except HttpError as error:
            self.fail(error.responseText)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(parents=[AnsibleIntegrationTest.parser])
    parser.add_argument('-i', '--aws_access_key_id', help='', required=True)
    parser.add_argument('-a', '--aws_secret_access_key', help='', required=True)

    args = parser.parse_args()

    suite = unittest.TestSuite()
    suite.addTest(EC2IntegrationTest('test', args.girder_url, args.girder_user,
        args.girder_password, args.aws_access_key_id, args.aws_secret_access_key))
    unittest.TextTestRunner().run(suite)
