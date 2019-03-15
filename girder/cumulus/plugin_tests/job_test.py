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


class JobTestCase(base.TestCase):

    def setUp(self):
        super(JobTestCase, self).setUp()

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

    def test_create(self):
        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'commands': [
                ''
            ],
            'name': '',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)

        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        # Name can't be empty
        self.assertStatus(r, 400)

        # Now try with valid name
        body['name'] = 'testing'
        json_body = json.dumps(body)

        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        del r.json['_id']

        expected_job = {u'status': u'created', u'userId': str(self._user['_id']),  u'commands': [u''], u'name': u'testing', u'onComplete': {u'cluster': u'terminate'}, u'output': [{
            u'itemId': u'546a1844ff34c70456111185'}], u'input': [{u'itemId': u'546a1844ff34c70456111185', u'path': u''}]}
        self.assertEqual(r.json, expected_job)

        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'name': 'test',
            'output': {
                'itemId': '546a1844ff34c70456111185'
            }
        }

        json_body = json.dumps(body)

        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        # Must provide commands or jobId
        self.assertStatus(r, 400)

        # Create test script
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

        # Create job using script
        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'onTerminate': {
                'scriptId': script_id
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'scriptId': script_id,
            'name': 'test',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        # Doesn't have access to script
        self.assertStatus(r, 403)

        # Create with correct user
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._cumulus)

        self.assertStatus(r, 201)
        expected_job = {u'status': u'created', u'userId': str(self._cumulus['_id']), u'commands': [u'echo "test"'], u'name': u'test', u'onComplete': {u'cluster': u'terminate'},  u'onTerminate': {'commands': [u'echo "test"']}, u'output': [{
            u'itemId': u'546a1844ff34c70456111185'}], u'input': [{u'itemId': u'546a1844ff34c70456111185', u'path': u''}]}
        del r.json['_id']
        self.assertEqual(r.json, expected_job)

        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'scriptId': '546a1844ff34c70456111185',
            'name': 'test',
            'output': {
                'itemId': '546a1844ff34c70456111185'
            }
        }

        json_body = json.dumps(body)
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._cumulus)

        # Bogus script id
        self.assertStatus(r, 400)

    def test_create_job_name_check(self):
        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': 'test'
                }
            ],
            'commands': ['echo "test"'],
            'name': 'test',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._cumulus)

        # input path and name can't be that same
        self.assertStatus(r, 400)

    def test_get(self):
        r = self.request(
            '/jobs/546a1844ff34c70456111185', method='GET', user=self._cumulus)
        self.assertStatus(r, 404)

        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'commands': [
                ''
            ],
            'name': 'test',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        job_id = r.json['_id']
        r = self.request('/jobs/%s' %
                         str(job_id), method='GET', user=self._cumulus)
        self.assertStatusOk(r)
        expected_job = {u'status': u'created', u'userId': str(self._user['_id']), u'commands': [u''], u'name': u'test', u'onComplete': {u'cluster': u'terminate'}, u'output': [{
            u'itemId': u'546a1844ff34c70456111185'}], u'input': [{u'itemId': u'546a1844ff34c70456111185', u'path': u''}],
            '_id': str(job_id)}
        self.assertEqual(r.json, expected_job)

    def test_update(self):
        status_body = {
            'status': 'testing'
        }

        r = self.request(
            '/jobs/546a1844ff34c70456111185', method='PATCH',
            type='application/json', body=json.dumps(status_body),
            user=self._cumulus)

        self.assertStatus(r, 404)

        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'commands': [
                ''
            ],
            'name': 'test',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        job_id = r.json['_id']

        r = self.request('/jobs/%s' % str(job_id), method='PATCH',
                         type='application/json', body=json.dumps(status_body),
                         user=self._cumulus)
        self.assertStatusOk(r)
        expected_job = {u'status': u'testing', u'userId': str(self._user['_id']), u'commands': [u''], u'name': u'test', u'onComplete': {u'cluster': u'terminate'}, u'output': [{
            u'itemId': u'546a1844ff34c70456111185'}], u'input': [{u'itemId': u'546a1844ff34c70456111185', u'path': u''}],
            '_id': str(job_id)}
        self.assertEqual(r.json, expected_job)

        # Check we get the right server side events
        r = self.request('/notification/stream', method='GET', user=self._user,
                         isJson=False, params={'timeout': 0})
        self.assertStatusOk(r)
        notifications = self.getSseMessages(r)
        self.assertEqual(len(notifications), 2, 'Expecting two notifications')
        notification = notifications[1]
        notification_type = notification['type']
        data = notification['data']
        self.assertEqual(notification_type, 'job.status')
        expected = {
            u'status': u'testing',
            u'_id': job_id
        }
        self.assertEqual(data, expected, 'Unexpected notification data')

        body = {
            'metadata': {
                'my': 'data'
            }
        }

        # Test update metadata property
        r = self.request('/jobs/%s' % str(job_id), method='PATCH',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)

        self.assertTrue('metadata' in r.json)
        self.assertEqual(r.json['metadata'], body['metadata'])

        # Update again
        body = {
            'metadata': {
                'my': 'data2',
                'new': 1
            }
        }
        r = self.request('/jobs/%s' % str(job_id), method='PATCH',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)

        self.assertTrue('metadata' in r.json)
        self.assertEqual(r.json['metadata'], body['metadata'])

    def test_log(self):
        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'commands': [
                ''
            ],
            'name': 'test',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        job_id = r.json['_id']

        log_entry = {
            'msg': 'Some message'
        }

        r = self.request('/jobs/546a1844ff34c70456111185/log', method='GET',
                         user=self._user)
        self.assertStatus(r, 404)

        r = self.request('/jobs/%s/log' % str(job_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        r = self.request('/jobs/%s/log' % str(job_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        expected_log = {u'log': [{u'msg': u'Some message'}]}
        self.assertEqual(r.json, expected_log)

        r = self.request('/jobs/%s/log' % str(job_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        r = self.request('/jobs/%s/log' % str(job_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json['log']), 2)

        r = self.request('/jobs/%s/log' % str(job_id), method='GET',
                         params={'offset': 1}, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json['log']), 1)

    def test_get_status(self):
        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'commands': [
                ''
            ],
            'name': 'test',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        job_id = r.json['_id']

        r = self.request('/jobs/%s/status' %
                         str(job_id), method='GET', user=self._user)
        self.assertStatusOk(r)
        expected_status = {u'status': u'created'}
        self.assertEqual(r.json, expected_status)

    def test_delete(self):
        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'commands': [
                ''
            ],
            'name': 'test',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        job_id = r.json['_id']

        r = self.request('/jobs/%s' %
                         str(job_id), method='DELETE', user=self._cumulus)
        self.assertStatusOk(r)

        r = self.request('/jobs/%s' %
                         str(job_id), method='GET', user=self._cumulus)
        self.assertStatus(r, 404)

    def test_list(self):
        def create_job(user, name):
            body = {
                'onComplete': {
                    'cluster': 'terminate'
                },
                'input': [
                    {
                        'itemId': '546a1844ff34c70456111185',
                        'path': ''
                    }
                ],
                'commands': [
                    ''
                ],
                'name': name,
                'output': [{
                    'itemId': '546a1844ff34c70456111185'
                }]
            }

            json_body = json.dumps(body)
            r = self.request('/jobs', method='POST',
                             type='application/json', body=json_body, user=user)

            self.assertStatus(r, 201)

            return r.json

        create_job(self._user, 'test0')
        job1 = create_job(self._another_user, 'test1')
        job2 = create_job(self._another_user, 'test2')

        r = self.request('/jobs', method='GET',
                         type='application/json', user=self._another_user)

        self.assertStatus(r, 200)
        self.assertEqual(len(r.json), 2)
        job_ids = [job['_id'] for job in r.json]
        self.assertTrue(job1['_id'] in job_ids)
        self.assertTrue(job2['_id'] in job_ids)

        # Now test limit
        params = {
            'limit': 1
        }
        r = self.request('/jobs', method='GET',
                         type='application/json', params=params,
                         user=self._another_user)

        self.assertStatus(r, 200)
        self.assertEqual(len(r.json), 1)
        self.assertEqual(r.json[0]['_id'], job1['_id'])


        # Now test offset
        params = {
            'offset': 1
        }
        r = self.request('/jobs', method='GET',
                         type='application/json', params=params,
                         user=self._another_user)

        self.assertStatus(r, 200)
        self.assertEqual(len(r.json), 1)
        self.assertEqual(r.json[0]['_id'], job2['_id'])

    def test_delete_running(self):
        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'commands': [
                ''
            ],
            'name': 'test',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        job_id = r.json['_id']

        body = {
            'status': 'running'
        }

        json_body = json.dumps(body)
        r = self.request('/jobs/%s' %
                         str(job_id), method='PATCH',
                         type='application/json', body=json_body,
                         user=self._cumulus)
        self.assertStatusOk(r)

        r = self.request('/jobs/%s' %
                 str(job_id), method='DELETE', user=self._cumulus)
        self.assertStatus(r, 400)

    def test_job_sse(self):
        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'commands': [
                ''
            ],
            'name': 'test',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)

        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        job_id = r.json['_id']

        # connect to cluster notification stream
        stream_r = self.request('/notification/stream', method='GET', user=self._user,
                         isJson=False, params={'timeout': 0})
        self.assertStatusOk(stream_r)

        # add a log entry
        log_entry = {
            'msg': 'Some message'
        }
        r = self.request('/jobs/%s/log' % str(job_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        notifications = self.getSseMessages(stream_r)
        # we get 2 notifications, 1 from the creation and 1 from the log
        self.assertEqual(len(notifications), 2, 'Expecting two notification, received %d' % len(notifications))
        self.assertEqual(notifications[0]['type'], 'job.status', 'Expecting a message with type \'job.status\'')
        self.assertEqual(notifications[1]['type'], 'job.log', 'Expecting a message with type \'job.log\'')
