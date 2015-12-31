import argparse
import unittest
import json
import time
import os
import traceback
import tempfile
import hashlib

from girder_client import GirderClient, HttpError

class BaseIntegrationTest(unittest.TestCase):
    def __init__(self, name, girder_url, girder_user, girder_password):
        super(BaseIntegrationTest, self).__init__(name)
        self.girder_url = girder_url
        self.girder_user = girder_user
        self.girder_password = girder_password

    def setUp(self):
        url = '%s/api/v1' % self.girder_url
        self._client = GirderClient(apiUrl=url)
        self._client.authenticate(self.girder_user,
                                  self.girder_password)

        user = self._client.get('user/me')
        self._user_id = user['_id']
        r = self._client.listFolder(self._user_id, 'user', name='Private')
        self.assertEqual(len(r), 1)
        self._private_folder_id = r[0]['_id']

    def tearDown(self):
        if self._job_id:
            try:
                url = 'jobs/%s' % self._job_id
                self._client.delete(url)
            except Exception:
                traceback.print_exc()

        if self._script_id:
            try:
                url = 'scripts/%s' % self._script_id
                self._client.delete(url)
            except Exception:
                traceback.print_exc()

        if self._item_id:
            try:
                url = 'item/%s' % self._item_id
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

    def create_script(self):
        body = {
            'commands': [
                'sleep 10', 'cat input/CumulusIntegrationTestInput'
            ],
            'name': 'CumulusIntegrationTestLob'
        }

        r = self._client.post('scripts', data=json.dumps(body))
        self._script_id = r['_id']

    def create_input(self):

        r = self._client.createFolder(self._private_folder_id, 'CumulusInput')
        self._input_folder_id = r['_id']
        size = os.path.getsize(__file__)

        with open(__file__, 'r') as fp:
            item = self._client.uploadFile(self._input_folder_id,
                    fp, 'CumulusIntegrationTestInput', size, parentType='folder')

        self._item_id = item['itemId']

    def create_output_folder(self):
        r = self._client.createFolder(self._private_folder_id, 'CumulusOutput')
        self._output_folder_id = r['_id']

    def create_job(self):
        body = {
            'name': 'CumulusIntegrationTestJob',
            'scriptId': self._script_id,
            'output': [{
              'folderId': self._output_folder_id,
              'path': '.'
            }],
            'input': [
              {
                'folderId': self._input_folder_id,
                'path': 'input'
              }
            ]
        }

        job = self._client.post('jobs', data=json.dumps(body))
        self._job_id = job['_id']

    def submit_job(self):
        url = 'clusters/%s/job/%s/submit' % (self._cluster_id, self._job_id)
        self._client.put(url)
        sleeps = 0
        while True:
            time.sleep(1)
            r = self._client.get('jobs/%s' % self._job_id)

            if r['status'] == 'error':
                r = self._client.get('jobs/%s/log' % self._job_id)
                self.fail(str(r))
            elif r['status'] == 'complete':
                break

            if sleeps > 30:
                self.fail('Cluster never moved into created state')
            sleeps += 1

    def assert_output(self):
        r = self._client.listItem(self._output_folder_id)
        self.assertEqual(len(r), 3)

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
            def md5(p):
                with open(p, 'r') as fp:
                    m = hashlib.md5()
                    m.update(fp.read())
                    return m.digest()

            self.assertEqual(md5(path), md5(__file__))

        finally:
            if os.path.exists(path):
                os.remove(path)

    def test(self):
        try:
            self.create_cluster()
            self.create_script()
            self.create_input()
            self.create_output_folder()
            self.create_job()
            self.submit_job()
            self.assert_output()
        except HttpError as error:
            self.fail(error.responseText)

base_parser = argparse.ArgumentParser(description='Run integration test',
                                      add_help=False)
base_parser.add_argument('-g', '--girder_user', help='', required=True)
base_parser.add_argument('-p', '--girder_password', help='', required=True)
base_parser.add_argument('-r', '--girder_url', help='', required=True)




