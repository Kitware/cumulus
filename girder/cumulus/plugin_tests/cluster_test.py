from tests import base
import json
import mock
import re


def setUpModule():
    base.enabledPlugins.append('cumulus')
    base.startServer()


def tearDownModule():
    base.stopServer()


class ClusterTestCase(base.TestCase):

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
        super(ClusterTestCase, self).setUp()

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

        # Create a config to use
        config = {u'permission': [{u'http': {u'to_port': u'80', u'from_port': u'80', u'ip_protocol': u'tcp'}}, {u'http8080': {u'to_port': u'8080', u'from_port': u'8080', u'ip_protocol': u'tcp'}}, {u'https': {u'to_port': u'443', u'from_port': u'443', u'ip_protocol': u'tcp'}}, {u'paraview': {u'to_port': u'11111', u'from_port': u'11111', u'ip_protocol': u'tcp'}}, {u'ssh': {u'to_port': u'22', u'from_port': u'22', u'ip_protocol': u'tcp'}}], u'global': {u'default_template': u''}, u'aws': [{u'info': {u'aws_secret_access_key': u'3z/PSglaGt1MGtGJ', u'aws_region_name': u'us-west-2', u'aws_region_host': u'ec2.us-west-2.amazonaws.com', u'aws_access_key_id': u'AKRWOVFSYTVQ2Q', u'aws_user_id': u'cjh'}}], u'cluster': [
            {u'default_cluster': {u'availability_zone': u'us-west-2a', u'master_instance_type': u't1.micro', u'node_image_id': u'ami-b2badb82', u'cluster_user': u'ubuntu', u'public_ips': u'True', u'keyname': u'cjh', u'cluster_size': u'2', u'plugins': u'requests-installer', u'node_instance_type': u't1.micro', u'permissions': u'ssh, http, paraview, http8080'}}], u'key': [{u'cjh': {u'key_location': u'/home/cjh/work/source/cumulus/cjh.pem'}}], u'plugin': [{u'requests-installer': {u'setup_class': u'starcluster.plugins.pypkginstaller.PyPkgInstaller', u'packages': u'requests, requests-toolbelt'}}]}

        config_body = {
            'name': 'test',
            'config': config
        }

        r = self.request('/starcluster-configs', method='POST',
                         type='application/json', body=json.dumps(config_body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        self._config_id = r.json['_id']

    def test_create(self):
        body = {
            'config': [
                {
                    '_id': ''
                }
            ],
            'name': '',
            'template': 'default_cluster'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)

        body['config'][0]['_id'] = '546a1844ff34c70456111185'
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)

        body['config'][0]['_id'] = str(self._config_id)
        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)

        body['name'] = 'mycluster'
        json_body = json.dumps(body)
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)

        # Try invalid template name
        body['template'] = 'mycluster'
        json_body = json.dumps(body)
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)

    def test_get(self):
        body = {
            'config': [
                {
                    '_id': self._config_id
                }
            ],
            'name': 'test',
            'template': 'default_cluster'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        r = self.request('/clusters/%s' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)

        expected_cluster = {
            u'status': u'created',
            u'_id': cluster_id,
            u'name': u'test',
            u'template': u'default_cluster',
            u'type': u'ec2'
        }
        config_id = r.json['configId']
        del r.json['configId']
        self.assertEqual(r.json, expected_cluster)

        # Ensure user can get full config
        r = self.request('/starcluster-configs/%s' % str(config_id),
                         method='GET', user=self._user)
        self.assertStatus(r, 403)

        # Check for 404
        r = self.request('/clusters/546a1844ff34c70456111185', method='GET',
                         user=self._user)
        self.assertStatus(r, 404)

    def test_update(self):
        status_body = {
            'status': 'testing'
        }

        r = self.request(
            '/clusters/546a1844ff34c70456111185', method='PATCH',
            type='application/json', body=json.dumps(status_body),
            user=self._cumulus)

        self.assertStatus(r, 404)

        body = {
            'config': [
                {
                    '_id': self._config_id
                }
            ],
            'name': 'test',
            'template': 'default_cluster'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']
        config_id = r.json['configId']

        r = self.request(
            '/clusters/%s' % str(cluster_id), method='PATCH',
            type='application/json', body=json.dumps(status_body),
            user=self._cumulus)

        self.assertStatusOk(r)
        expected_cluster = {u'status': u'testing', u'configId': config_id,
                            u'_id': cluster_id, u'name': u'test', u'template': u'default_cluster', u'type': u'ec2'}
        self.assertEqual(r.json, expected_cluster)

        # Test GET status
        r = self.request('/clusters/%s' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        expected_status = {u'status': u'testing', u'configId': config_id,
                           u'_id': cluster_id, u'name': u'test', u'template': u'default_cluster', u'type': u'ec2'}
        self.assertEquals(r.json, expected_status)

    def test_log(self):
        body = {
            'config': [
                {
                    '_id': self._config_id
                }
            ],
            'name': 'test',
            'template': 'default_cluster'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        log_entry = {
            'msg': 'Some message'
        }

        r = self.request('/clusters/546a1844ff34c70456111185/log', method='GET',
                         user=self._user)
        self.assertStatus(r, 404)

        r = self.request('/clusters/%s/log' % str(cluster_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        r = self.request('/clusters/%s/log' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        expected_log = {u'log': [{u'msg': u'Some message'}]}
        self.assertEquals(r.json, expected_log)

        r = self.request('/clusters/%s/log' % str(cluster_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        r = self.request('/clusters/%s/log' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        self.assertEquals(len(r.json['log']), 2)

        r = self.request('/clusters/%s/log' % str(cluster_id), method='GET',
                         params={'offset': 1}, user=self._user)
        self.assertStatusOk(r)
        self.assertEquals(len(r.json['log']), 1)

    @mock.patch('cumulus.starcluster.tasks.cluster.start_cluster.delay')
    def test_start(self, start_cluster):
        body = {
            'config': [
                {
                    '_id': self._config_id
                }
            ],
            'name': 'test',
            'template': 'default_cluster'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']
        config_id = r.json['configId']

        r = self.request('/clusters/%s/start' % str(cluster_id), method='PUT',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatusOk(r)

        expected_start_call = [[[{u'status': u'created', u'configId': config_id, u'_id': cluster_id, u'name': u'test', u'template': u'default_cluster', u'type': u'ec2'}], {
            u'on_start_submit': None, u'girder_token': u'token', u'log_write_url': u'http://127.0.0.1/api/v1/clusters/%s/log' % str(cluster_id)}]]
        self.assertCalls(start_cluster.call_args_list, expected_start_call)

    @mock.patch('cumulus.starcluster.tasks.job.submit')
    def test_submit_job(self, submit):
        body = {
            'config': [
                {
                    '_id': self._config_id
                }
            ],
            'name': 'test',
            'template': 'default_cluster'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']
        config_id = r.json['configId']

        # Create a job
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

        r = self.request('/clusters/%s/job/%s/submit' % (str(cluster_id), str(job_id)), method='PUT',
                         type='application/json', body={}, user=self._user)
        expected_response = {
            u'message': u'Cluster is not running', u'type': u'rest'}
        self.assertEquals(r.json, expected_response)
        self.assertStatus(r, 400)

        # Move cluster into running state
        status_body = {
            'status': 'running'
        }

        r = self.request(
            '/clusters/%s' % str(cluster_id), method='PATCH',
            type='application/json', body=json.dumps(status_body),
            user=self._cumulus)

        self.assertStatusOk(r)

        r = self.request('/clusters/%s/job/%s/submit' % (str(cluster_id), str(job_id)), method='PUT',
                         type='application/json', body={}, user=self._user)
        self.assertStatusOk(r)

        expected_submit_call = [[[u'token', {u'status': u'running', u'configId': config_id, u'_id': cluster_id, u'name': u'test', u'template': u'default_cluster', u'type': u'ec2'}, {u'status': u'created', u'commands': [u''], u'name': u'test', u'onComplete': {u'cluster': u'terminate'}, u'clusterId': cluster_id, u'input': [
            {u'itemId': u'546a1844ff34c70456111185', u'path': u''}], u'output': [{u'itemId': u'546a1844ff34c70456111185'}], u'_id': job_id, u'log': []}, u'http://127.0.0.1/api/v1/jobs/%s/log' % job_id, u'http://127.0.0.1/api/v1/starcluster-configs/%s?format=ini' % config_id], {}]]
        self.assertCalls(submit.call_args_list, expected_submit_call)

    @mock.patch('cumulus.starcluster.tasks.cluster.terminate_cluster.delay')
    def test_terminate(self, terminate_cluster):
        body = {
            'config': [
                {
                    '_id': self._config_id
                }
            ],
            'name': 'test',
            'template': 'default_cluster'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']
        config_id = r.json['configId']

        # Move cluster into running state
        status_body = {
            'status': 'running'
        }

        r = self.request(
            '/clusters/%s/terminate' % str(cluster_id), method='PUT',
            type='application/json', body=json.dumps(status_body),
            user=self._cumulus)

        self.assertStatusOk(r)

        expected_terminate_call = [[[{u'status': u'created', u'configId': config_id, u'_id': str(cluster_id),
                                      u'name': u'test', u'template': u'default_cluster', u'type': u'ec2'}], {u'girder_token': u'token', u'log_write_url': u'http://127.0.0.1/api/v1/clusters/%s/log' % str(cluster_id)}]]

        self.assertCalls(
            terminate_cluster.call_args_list, expected_terminate_call)

    def test_delete(self):
        body = {
            'config': [
                {
                    '_id': self._config_id
                }
            ],
            'name': 'test',
            'template': 'default_cluster'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        r = self.request('/clusters/%s' %
                         str(cluster_id), method='DELETE', user=self._cumulus)
        self.assertStatusOk(r)

        r = self.request('/clusters/%s' %
                         str(cluster_id), method='GET', user=self._cumulus)
        self.assertStatus(r, 404)

    def test_create_invalid_type(self):
        body = {
            'type': 'bogus'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)
        self.assertEqual(r.json, {u'message': u'Invalid cluster type.', u'type': u'rest'}, 'Unexpected error message')

    def test_create_trad_type(self):
        body = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'username': 'bob',
                'hostname': 'myhost'
            }
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)

    def test_start_trad(self):
        body = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'username': 'bob',
                'hostname': 'myhost'
            }
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        _id = str(r.json['_id'])
        r = self.request('/clusters/%s/start' % _id, method='PUT',
                         type='application/json', body={}, user=self._user)

        self.assertStatus(r, 400)

    def test_terminate_trad(self):
        body = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'username': 'bob',
                'hostname': 'myhost'
            }
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        _id = str(r.json['_id'])
        r = self.request('/clusters/%s/terminate' % _id, method='PUT',
                         type='application/json', body={}, user=self._user)

        self.assertStatus(r, 400)

