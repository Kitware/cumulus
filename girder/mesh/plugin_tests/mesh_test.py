from tests import base
import json
import os
import mock
import re


def setUpModule():
    base.enabledPlugins.append('mesh')
    base.startServer()


def tearDownModule():
    base.stopServer()


class MeshTestCase(base.TestCase):

    def normalize(self, data):
        str_data = json.dumps(data, default=str)
        str_data = re.sub(r'[\w]{64}', 'token', str_data)

        return json.loads(str_data)

    def assertCalls(self, actual, expected, msg=None):
        calls = []
        for (args, kwargs) in self.normalize(actual):
            calls.append((args, kwargs))

        self.assertListEqual(self.normalize(calls), expected, msg)

    def setUp(self):
        super(MeshTestCase, self).setUp()

        self._proxy_file_path = '/tmp/proxy'
        full_path = '%s.db' % self._proxy_file_path
        if os.path.exists(full_path):
            os.remove(full_path)

        users = ({
            'email': 'cumulus@email.com',
            'login': 'cumulus',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        }, {
            'email': 'regularuser@email.com',
            'login': 'regularuser',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        }, {
            'email': 'another@email.com',
            'login': 'another',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        })
        self._cumulus, self._user, self._another_user = \
            [self.model('user').createUser(**user) for user in users]

        self._group = self.model('group').createGroup('cumulus', self._cumulus)

        # Add test mesh
        params = {
            'name': 'test'
        }
        r = self.request(
            '/collection', method='POST', params=params, user=self._cumulus)
        self.assertStatusOk(r)

        collection_id = r.json['_id']

        params = {
            'parentType': 'collection',
            'parentId': collection_id,
            'name': 'test'
        }
        r = self.request(
            '/folder', method='POST', params=params, user=self._cumulus)
        self.assertStatusOk(r)

        folder_id = r.json['_id']

        params = {
            'folderId': folder_id,
            'name': 'mesh'

        }
        r = self.request(
            '/item', method='POST', params=params, user=self._cumulus)
        self.assertStatusOk(r)

        self.item_id = r.json['_id']

        data = os.urandom(2048)

        params = {
            'parentType': 'item',
            'parentId': self.item_id,
            'name': 'mesh.in',
            'size': len(data)
        }
        r = self.request(
            '/file', method='POST', params=params, user=self._cumulus)
        self.assertStatusOk(r)

        print r.json

        self.file_id = r.json['_id']

        fields = [('offset', 0), ('uploadId', self.file_id)]
        files = [('chunk', 'mesh.in', data)]
        r = self.multipartRequest(
            path='/file/chunk', fields=fields, files=files, user=self._cumulus)
        self.assertStatusOk(r)

        self.file_id = r.json['_id']

    @mock.patch('cumulus.starcluster.tasks.celery.command.send_task')
    def test_extract_surface(self, send_task):
        body = {
        }

        json_body = json.dumps(body)

        url = '/meshes/%s/extract/surface' % str(self.file_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json_body, user=self._cumulus)
        self.assertStatus(r, 400)
        body['output'] = {}
        json_body = json.dumps(body)
        r = self.request(url, method='PUT',
                         type='application/json', body=json_body, user=self._cumulus)
        self.assertStatus(r, 400)
        body['output']['itemId'] = self.item_id
        json_body = json.dumps(body)
        r = self.request(url, method='PUT',
                         type='application/json', body=json_body, user=self._cumulus)
        self.assertStatus(r, 400)
        body['output']['name'] = 'mesh.in'
        json_body = json.dumps(body)
        r = self.request(url, method='PUT',
                         type='application/json', body=json_body, user=self._cumulus)
        self.assertStatusOk(r)

        expected_calls = [[[u'cumulus.moab.tasks.mesh.extract'], {u'args': [
            u'token', self.file_id, {u'itemId': self.item_id, u'name': u'mesh.in'}]}]]
        self.assertCalls(send_task.call_args_list, expected_calls)

        bogus_id = '54789ef6ff34c72d45f1ed11'
        json_body = json.dumps(body)
        r = self.request('/meshes/%s/extract/surface' % bogus_id, method='PUT',
                         type='application/json', body=json_body, user=self._cumulus)
        self.assertStatus(r, 400)

        bogus_id = '54789ef6ff34c72d45f1ed11'
        body['output']['itemId'] = bogus_id
        json_body = json.dumps(body)
        r = self.request(url, method='PUT',
                         type='application/json', body=json_body, user=self._cumulus)
        self.assertStatus(r, 400)
