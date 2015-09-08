from tests import base
import json
import mock
import re
from easydict import EasyDict


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

        self._valid_key = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDJ0wahxwaNbCDdbRll9FypQRXQv5PXQSTh1IeSynTcZZWSyQH4JhoI0lb3/IW7GllIkWblEuyv2SHzXMKRaaFuwmnU1zsY6Y55N6DJt0e9TvieT8MfaM2e7qqaN+0RS2aFb8iw3i+G80tmFVJWuNm7AITVVPf60Nbc5Bgk9qVIa4BakJ3SmW0p/iHT3CStb/k+psevFYyYCEw5l3+3ejPh9b/3423yRzq5r0cyOw8y8fIe4JV8MlE4z2huc/o9Xpw8mzNim7QdobNOylwJsvIYtB4d+MTqvsnt16e22BS/FKuTXx6jGRFFtYNWwwDQe9IIxYb6dPs1XPKVx081nRUwNjar2um41XUOhPx1N5+LfbrYkACVEZiEkW/Ph6hu0PsYQXbL00sWzrzIunixepn5c2dMnDvugvGQA54Z0EXgIYHnetJp2Xck1pJH6oNSSyA+5Mx5QAH5MFNL3YOnGxGBLrkUfK9Ff7QOiZdqXbZoXXS49WtL42Jsv8SgFu3w5NLffvD6/vCOBHwWxh+8VLg5n28M7pZ8+xyMBidkGkG9di2PfV4XsSAeoIc5utgbUJFT6URr2pW9KT4FxTq/easgiJFZUz48SNAjcBneElB9bjAaGf47BPfCNsIAWU2c9MZJWjURpWtzfk21k2/BAfBPs2VNb8dapY6dNinxLqbPIQ== your_email@example.com'

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

    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    def test_create_using_aws_profile(self, EasyEC2, generate_key_pair):
        # First create a profile
        instance = EasyEC2.return_value
        instance.get_region.return_value = EasyDict({'endpoint': 'cornwall.ec2.amazon.com'})

        body = {
            'name': 'myprof',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            'availabilityZone': 'cornwall-2b'
        }

        create_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        profile_id = str(r.json['_id'])

        # First test invalid profileId
        body = {
            'config': [
                {
                    '_id': self._config_id,
                    'aws': {
                        'profileId': '546a1844ff34c70456111385'
                    }
                }
            ],
            'name': 'mycluster',
            'template': 'default_cluster'
        }

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json.dumps(body), user=self._user)
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
        config_id = r.json['config']['_id']
        del r.json['config']
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
        config_id = r.json['config']['_id']

        r = self.request(
            '/clusters/%s' % str(cluster_id), method='PATCH',
            type='application/json', body=json.dumps(status_body),
            user=self._cumulus)

        self.assertStatusOk(r)
        expected_cluster = {u'status': u'testing', u'config': {u'_id': config_id},
                            u'_id': cluster_id, u'name': u'test', u'template': u'default_cluster', u'type': u'ec2'}
        self.assertEqual(r.json, expected_cluster)

        # Test GET status
        r = self.request('/clusters/%s' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        expected_status = {u'status': u'testing', u'config': {u'_id': config_id},
                           u'_id': cluster_id, u'name': u'test', u'template': u'default_cluster', u'type': u'ec2'}
        self.assertEquals(r.json, expected_status)

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_update_traditional(self, generate_key):
        body = {
            'config': {
                'host': 'myhost',
                'ssh': {
                    'user': 'myuser'
                }
            },
            'name': 'test',
            'type': 'trad'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        update_body = {
            'config': {
                'ssh': {
                    'publicKey': self._valid_key,
                    'passphrase': 'supersecret'
                }
            }
        }

        r = self.request(
            '/clusters/%s' % str(cluster_id), method='PATCH',
            type='application/json', body=json.dumps(update_body),
            user=self._cumulus)

        self.assertStatusOk(r)
        expected = {u'status': u'running', u'type': u'trad', u'_id': cluster_id, u'config': {
            u'host': u'myhost', u'ssh': {u'user': u'myuser', u'publicKey': self._valid_key}}, u'name': u'test'}
        self.assertEqual(
            self.normalize(expected), self.normalize(r.json), 'Unexpected response')

        r = self.request('/clusters/%s' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        expected = {u'status': u'running', u'type': u'trad', u'_id': cluster_id, u'config': {
            u'host': u'myhost', u'ssh': {u'user': u'myuser', u'publicKey': self._valid_key}}, u'name': u'test'}
        self.assertEqual(
            self.normalize(expected), self.normalize(r.json), 'Unexpected response')

        # Check that if we are in the right group we will get the passphrase
        r = self.request('/clusters/%s' % str(cluster_id), method='GET',
                         user=self._cumulus)
        self.assertStatusOk(r)
        expected = {u'status': u'running', u'type': u'trad', u'_id': cluster_id, u'config': {u'host': u'myhost', u'ssh': {
            u'user': u'myuser', u'publicKey': self._valid_key, u'passphrase': u'supersecret'}}, u'name': u'test'}
        self.assertEqual(
            self.normalize(expected), self.normalize(r.json), 'Unexpected response')

        # Check we get an error if we try and update in invalid key
        update_body = {
            'config': {
                'ssh': {
                    'publicKey': 'bogus',
                    'passphrase': 'supersecret'
                }
            }
        }
        r = self.request(
            '/clusters/%s' % str(cluster_id), method='PATCH',
            type='application/json', body=json.dumps(update_body),
            user=self._cumulus)
        self.assertStatus(r, 400)

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
        config_id = r.json['config']['_id']

        r = self.request('/clusters/%s/start' % str(cluster_id), method='PUT',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatusOk(r)

        expected_start_call = [[[{u'status': u'created', u'config': {u'_id': config_id}, u'_id': cluster_id, u'name': u'test', u'template': u'default_cluster', u'type': u'ec2'}], {
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
        config_id = r.json['config']['_id']

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

        expected_submit_call = [[[u'token', {u'status': u'running', u'config': {u'_id': config_id}, u'_id': cluster_id, u'name': u'test', u'template': u'default_cluster', u'type': u'ec2'}, {u'status': u'created', u'commands': [u''], u'name': u'test', u'onComplete': {u'cluster': u'terminate'}, u'clusterId': cluster_id, u'input': [
            {u'itemId': u'546a1844ff34c70456111185', u'path': u''}], u'output': [{u'itemId': u'546a1844ff34c70456111185'}], u'_id': job_id, u'log': []}, u'http://127.0.0.1/api/v1/jobs/%s/log' % job_id], {}]]
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
        config_id = r.json['config']['_id']

        # Move cluster into running state
        status_body = {
            'status': 'running'
        }

        r = self.request(
            '/clusters/%s/terminate' % str(cluster_id), method='PUT',
            type='application/json', body=json.dumps(status_body),
            user=self._cumulus)

        self.assertStatusOk(r)

        expected_terminate_call = [[[{u'status': u'created', u'config': {u'_id': config_id}, u'_id': str(cluster_id),
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

        # Test trying to delete a cluster that is running
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = str(r.json['_id'])

        status_body = {
            'status': 'running'
        }
        r = self.request(
            '/clusters/%s' % cluster_id, method='PATCH',
            type='application/json', body=json.dumps(status_body),
            user=self._user)
        self.assertStatusOk(r)

        r = self.request('/clusters/%s' %
                         cluster_id, method='DELETE', user=self._cumulus)
        self.assertStatus(r, 400)



    def test_create_invalid_type(self):
        body = {
            'type': 'bogus'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)
        self.assertEqual(r.json, {
                         u'message': u'Invalid cluster type.', u'type': u'rest'}, 'Unexpected error message')

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_create_trad_type(self, generate_key):
        body = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'ssh': {
                    'user': 'bob'
                },
                'host': 'myhost'
            }
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        cluster_id = r.json['_id']
        expected = [[[{u'status': u'running', u'type': u'trad', u'_id': cluster_id, u'config': {
            u'host': u'myhost', u'ssh': {u'user': u'bob'}}, u'name': u'my trad cluster'}, u'token'], {}]]
        self.assertCalls(
            generate_key.call_args_list, expected)

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_start_trad(self, generate_key):
        body = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'ssh': {
                    'user': 'bob'
                },
                'host': 'myhost'
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

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_terminate_trad(self, generate_key):
        body = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'ssh': {
                    'user': 'bob'
                },
                'host': 'myhost'
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

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_find(self, generate_key):
        # Create a EC2 cluster
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
        ec2_cluster_id = r.json['_id']
        ec2_cluster_config_id = r.json['config']['_id']

        # Create a traditional cluster
        body = {
            'config': {
                'ssh': {
                    'user': 'billy'
                },
                'host': 'home'
            },
            'name': 'trad_test',
            'type': 'trad'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        trad_cluster_id = r.json['_id']

        # Search for the EC2 cluster
        params = {
            'type': 'ec2'
        }
        r = self.request(
            '/clusters', method='GET', params=params, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 1, 'Only expecting a single cluster')
        expected_cluster = {
            u'status': u'created',
            u'type': u'ec2',
            u'template': u'default_cluster',
            u'_id': ec2_cluster_id,
            u'config': {
                u'_id': ec2_cluster_config_id
            },
            u'name': u'test'
        }
        self.assertEqual(r.json[0], expected_cluster, 'Return cluster doesn\'t match')

        # Search for the trad cluster
        params = {
            'type': 'trad'
        }
        r = self.request(
            '/clusters', method='GET', params=params, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 1, 'Only expecting a single cluster')
        expected_cluster = {
            u'status': u'running',
            u'type': u'trad',
            u'_id': trad_cluster_id,
            u'config': {
                u'host': u'home',
                u'ssh': {
                    u'user': u'billy'
                }
            },
            u'name': u'trad_test'
        }
        self.assertEqual(r.json[0], expected_cluster, 'Return cluster doesn\'t match')

        # Check limit works
        r = self.request(
            '/clusters', method='GET', params={}, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 2, 'Two clusters expected')

        params = {
            'limit': 1
        }
        r = self.request(
            '/clusters', method='GET', params=params, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 1, 'One cluster expected')

        # Checkout that only owner can see clusters
        r = self.request(
            '/clusters', method='GET', params={}, user=self._another_user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 0, 'Don\'t expect any clusters')

