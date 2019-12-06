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

from tests import base
import json

from girder.utility.model_importer import ModelImporter

def setUpModule():
    base.enabledPlugins.append('cumulus')
    base.startServer()


def tearDownModule():
    base.stopServer()


class ScriptTestCase(base.TestCase):

    def setUp(self):
        super(ScriptTestCase, self).setUp()

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
        })
        self._cumulus, self._user = \
            [ModelImporter.model('user').createUser(**user) for user in users]

        self._group = ModelImporter.model('group').createGroup('cumulus', self._cumulus)

    def test_create(self):
        body = {
            'commands': ['echo "test"'],
            'name': 'test'
        }
        body = json.dumps(body)

        r = self.request('/scripts', method='POST',
                         type='application/json', body=body, user=self._cumulus)
        self.assertStatus(r, 201)

        self.assertEqual(r.json['commands'], ['echo "test"'])
        self.assertEqual(r.json['name'], 'test')

        body = {
            'commands': ['echo "test"'],
        }
        body = json.dumps(body)

        r = self.request('/scripts', method='POST',
                         type='application/json', body=body, user=self._cumulus)
        self.assertStatus(r, 400)

    def test_import(self):
        body = {
            'name': 'test'
        }
        body = json.dumps(body)

        r = self.request('/scripts', method='POST',
                         type='application/json', body=body, user=self._cumulus)
        self.assertStatus(r, 201)

        self.assertEqual(r.json['name'], 'test')
        script_id = r.json['_id']

        r = self.request('/scripts/546a1844ff34c70456111185/import', method='PATCH',
                         type='text/plain', body='echo "test1"\necho "test2"', user=self._cumulus)
        self.assertStatus(r, 404)

        r = self.request('/scripts/%s/import' % str(script_id), method='PATCH',
                         type='text/plain', body='echo "test1"\necho "test2"', user=self._cumulus)
        self.assertStatusOk(r)
        expected_script = {
            'commands': [
                'echo "test1"',
                'echo "test2"'
            ],
            'name': 'test'
        }
        del r.json['_id']
        self.assertEqual(r.json, expected_script)



    def test_get(self):
        body = {
            'commands': ['echo "test"'],
            'name': 'test'
        }
        body = json.dumps(body)

        r = self.request('/scripts', method='POST',
                         type='application/json', body=body, user=self._cumulus)
        self.assertStatus(r, 201)

        self.assertEqual(r.json['commands'], ['echo "test"'])
        self.assertEqual(r.json['name'], 'test')
        script_id = r.json['_id']

        r = self.request('/scripts/546a1844ff34c70456111185', method='GET',
                         user=self._cumulus)
        self.assertStatus(r, 404)

        r = self.request('/scripts/%s' % str(script_id), method='GET',
                         user=self._cumulus)
        self.assertStatusOk(r)
        expected_script = {
            'commands': [
                'echo "test"'
            ],
            'name': 'test'
        }
        del r.json['_id']
        self.assertEqual(r.json, expected_script)


    def test_delete(self):
        body = {
            'commands': ['echo "test"'],
            'name': 'test'
        }
        body = json.dumps(body)

        r = self.request('/scripts', method='POST',
                         type='application/json', body=body, user=self._cumulus)
        self.assertStatus(r, 201)

        self.assertEqual(r.json['commands'], ['echo "test"'])
        self.assertEqual(r.json['name'], 'test')
        script_id = r.json['_id']

        r = self.request('/scripts/%s' %
                         str(script_id), method='DELETE', user=self._user)
        self.assertStatus(r, 403)

        r = self.request('/scripts/%s' %
                         str(script_id), method='DELETE', user=self._cumulus)
        self.assertStatusOk(r)

        r = self.request('/scripts/%s' %
                         str(script_id), method='GET', user=self._cumulus)
        self.assertStatus(r, 404)
