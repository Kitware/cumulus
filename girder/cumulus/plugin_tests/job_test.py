from tests import base
import json


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
            [self.model('user').createUser(**user) for user in users]

        self._group = self.model('group').createGroup('cumulus', self._cumulus)

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
            'output': {
                'itemId': '546a1844ff34c70456111185'
            }
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

        # Name can't be empty
        self.assertStatus(r, 201)
        del r.json['_id']

        expected_job = {u'status': u'created', u'commands': [u''], u'name': u'testing', u'onComplete': {u'cluster': u'terminate'}, u'output': {
            u'itemId': u'546a1844ff34c70456111185'}, u'input': [{u'itemId': u'546a1844ff34c70456111185', u'path': u''}]}
        self.assertEqual(r.json, expected_job)

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
            'output': {
                'itemId': '546a1844ff34c70456111185'
            }
        }

        json_body = json.dumps(body)
        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        job_id = r.json['_id']
        r = self.request('/jobs/%s' %
                         str(job_id), method='GET', user=self._cumulus)
        self.assertStatusOk(r)
        expected_job = {u'status': u'created', u'commands': [u''], u'name': u'test', u'onComplete': {u'cluster': u'terminate'}, u'output': {
            u'itemId': u'546a1844ff34c70456111185'}, u'input': [{u'itemId': u'546a1844ff34c70456111185', u'path': u''}],
            '_id': str(job_id)}
        self.assertEquals(r.json, expected_job)

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
            'output': {
                'itemId': '546a1844ff34c70456111185'
            }
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
        expected_job = {u'status': u'testing', u'commands': [u''], u'name': u'test', u'onComplete': {u'cluster': u'terminate'}, u'output': {
            u'itemId': u'546a1844ff34c70456111185'}, u'input': [{u'itemId': u'546a1844ff34c70456111185', u'path': u''}],
            '_id': str(job_id)}
        self.assertEquals(r.json, expected_job)

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
            'output': {
                'itemId': '546a1844ff34c70456111185'
            }
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
        self.assertEquals(r.json, expected_log)

        r = self.request('/jobs/%s/log' % str(job_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        r = self.request('/jobs/%s/log' % str(job_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        self.assertEquals(len(r.json['log']), 2)

        r = self.request('/jobs/%s/log' % str(job_id), method='GET',
                         params={'offset': 1}, user=self._user)
        self.assertStatusOk(r)
        self.assertEquals(len(r.json['log']), 1)

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
            'output': {
                'itemId': '546a1844ff34c70456111185'
            }
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
        self.assertEquals(r.json, expected_status)

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
            'output': {
                'itemId': '546a1844ff34c70456111185'
            }
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
