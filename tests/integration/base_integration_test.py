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
import os
import traceback
import tempfile
import hashlib
import sys
from StringIO import StringIO

from girder_client import GirderClient, HttpError

class BaseIntegrationTest(unittest.TestCase):
    def __init__(self, name, girder_url, girder_user, girder_password, job_timeout=60, cleanup=True):
        super(BaseIntegrationTest, self).__init__(name)
        self._job_id = None
        self._script_id = None
        self._output_folder_id = None
        self._input_folder_id = None
        self._girder_url = girder_url
        self._girder_user = girder_user
        self._girder_password = girder_password
        self._job_timeout = job_timeout
        self._data = 'Need more input!'
        self._cleanup = cleanup

    def setUp(self):
        url = '%s/api/v1' % self._girder_url
        self._client = GirderClient(apiUrl=url)
        self._client.authenticate(self._girder_user,
                                  self._girder_password)

        user = self._client.get('user/me')
        self._user_id = user['_id']
        r = list(self._client.listFolder(self._user_id, 'user', name='Private'))
        self.assertEqual(len(r), 1)
        self._private_folder_id = r[0]['_id']

    def tearDown(self):

        if not self._cleanup:
            return

        if self._job_id:
            try:
                url = 'jobs/%s' % self._job_id
                self._client.delete(url)
            except Exception as e:
                traceback.print_exc()

        if self._script_id:
            try:
                url = 'scripts/%s' % self._script_id
                self._client.delete(url)
            except Exception:
                traceback.print_exc()

        if self._output_folder_id:
            try:
                url = 'folder/%s' % self._output_folder_id
                self._client.delete(url)
            except Exception:
                traceback.print_exc()

        if self._input_folder_id:
            try:
                url = 'folder/%s' % self._input_folder_id
                self._client.delete(url)
            except Exception:
                traceback.print_exc()

    def create_script(self, commands=[
                'sleep 10', 'cat CumulusIntegrationTestInput'
            ]):
        body = {
            'commands': commands,
            'name': 'CumulusIntegrationTestLob'
        }

        r = self._client.post('scripts', data=json.dumps(body))
        self._script_id = r['_id']

    def create_input(self, folder_name='CumulusInput'):

        r = self._client.createFolder(self._private_folder_id, folder_name)
        self._input_folder_id = r['_id']
        size = len(self._data)

        item = self._client.uploadFile(self._input_folder_id,
                    StringIO(self._data), 'CumulusIntegrationTestInput', size,
                    parentType='folder')

        self._item_id = item['itemId']

    def create_output_folder(self, folder_name='CumulusOutput'):
        r = self._client.createFolder(self._private_folder_id, folder_name)
        self._output_folder_id = r['_id']

    def create_job(self, job_name='CumulusIntegrationTestJob', tail=None):
        body = {
            'name': job_name,
            'scriptId': self._script_id,
            'output': [{
              'folderId': self._output_folder_id,
              'path': '.'
            }],
            'input': [
              {
                'folderId': self._input_folder_id,
                'path': '.'
              }
            ]
        }

        if tail:
            body['output'].append({
                "path": tail,
                "tail": True
            })

        job = self._client.post('jobs', data=json.dumps(body))
        self._job_id = job['_id']

    def submit_job(self, job_params={}, timeout=None):
        url = 'clusters/%s/job/%s/submit' % (self._cluster_id, self._job_id)

        self._client.put(url, data=json.dumps(job_params))
        start = time.time()
        while True:
            time.sleep(1)
            r = self._client.get('jobs/%s' % self._job_id)

            if r['status'] in ['error', 'unexpectederror']:
                r = self._client.get('jobs/%s/log' % self._job_id)
                self.fail(str(r))
            elif r['status'] == 'complete':
                break

            if time.time() - start > timeout:
                self.fail('Job didn\'t complete in timeout')

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

        path = os.path.join(tempfile.gettempdir(), self._job_id)
        try:
            self._client.downloadFile(r[0]['_id'], path)
            with open(path, 'rb') as fp:
                self.assertEqual(fp.read(), self._data)

        finally:
            if os.path.exists(path):
                os.remove(path)

BaseIntegrationTest.parser = argparse.ArgumentParser(description='Run integration test',
                                      add_help=False)
BaseIntegrationTest.parser.add_argument('-g', '--girder_user', help='', required=True)
BaseIntegrationTest.parser.add_argument('-p', '--girder_password', help='', required=True)
BaseIntegrationTest.parser.add_argument('-r', '--girder_url', help='', required=True)
