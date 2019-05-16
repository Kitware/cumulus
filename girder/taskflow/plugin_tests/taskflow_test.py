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

from tests import base
import json
import mock
import re
from jsonschema import validate
from cumulus.task import spec
from cumulus.task import runner

from girder.utility.model_importer import ModelImporter

def setUpModule():
    base.enabledPlugins.append('taskflow')
    base.startServer()


def tearDownModule():
    base.stopServer()


class TaskFlowTestCase(base.TestCase):

    def setUp(self):
        super(TaskFlowTestCase, self).setUp()

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
            [ModelImporter.model('user').createUser(**user) for user in users]

        self._group = ModelImporter.model('group').createGroup('cumulus', self._cumulus)

    def test_taskflow_sse(self):
        body = {
            'taskFlowClass': 'cumulus.taskflow.core.test.mytaskflows.SimpleTaskFlow',
            'name': 'test_taskflow'
        }

        json_body = json.dumps(body)

        r = self.request('/taskflows', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        taskflow_id = r.json['_id']

        # connect to cluster notification stream
        stream_r = self.request('/notification/stream', method='GET', user=self._user,
                                isJson=False, params={'timeout': 0})
        self.assertStatusOk(stream_r)

        # add a log entry
        log_entry = {
            'msg': 'Some message'
        }
        r = self.request('/taskflows/%s/log' % str(taskflow_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        notifications = self.getSseMessages(stream_r)
        # we get 2 notifications, 1 from the creation and 1 from the log
        self.assertEqual(len(notifications), 2, 'Expecting two notification, received %d' % len(notifications))
        self.assertEqual(notifications[0]['type'], 'taskflow.status', 'Expecting a message with type \'taskflow.status\'')
        self.assertEqual(notifications[1]['type'], 'taskflow.log', 'Expecting a message with type \'taskflow.log\'')

    def test_access(self):
        body = {
            'taskFlowClass': 'cumulus.taskflow.core.test.mytaskflows.SimpleTaskFlow',
            'name': 'test_taskflow'
        }

        json_body = json.dumps(body)

        r = self.request('/taskflows', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        taskflow_id = r.json['_id']

        # check empty access
        r = self.request('/taskflows/%s/access' % str(taskflow_id), method='GET',
                         type='application/json', user=self._user)
        self.assertStatus(r, 200)
        self.assertEqual(len(r.json['users']), 1)
        self.assertEqual(str(r.json['users'][0]['id']), str(self._user['_id']))

        # grant access to _another_user
        body = {'groups': [],
                'users': [str(self._another_user['_id'])]}
        json_body = json.dumps(body)
        r = self.request('/taskflows/%s/access' % str(taskflow_id), method='PATCH',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 200)

        # fetch the taskflow using one of the added users
        r = self.request('/taskflows/%s' % str(taskflow_id), method='GET',
                         type='application/json', user=self._another_user)
        self.assertStatus(r, 200)

        # revoke access of _another_user
        r = self.request('/taskflows/%s/access' % str(taskflow_id), method='PATCH',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 200)

        # fetch the taskflow using one of the added users
        r = self.request('/taskflows/%s' % str(taskflow_id), method='GET',
                         type='application/json', user=self._another_user)
        self.assertStatus(r, 403)
