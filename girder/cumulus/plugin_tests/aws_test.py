from tests import base
import json
import mock
import re
from easydict import EasyDict
from boto.exception import EC2ResponseError
from starcluster.exception import RegionDoesNotExist, ZoneDoesNotExist
import unittest

def setUpModule():
    base.enabledPlugins.append('cumulus')
    base.startServer()


def tearDownModule():
    base.stopServer()


class AwsTestCase(base.TestCase):

    def normalize(self, data):
        str_data = json.dumps(data, default=str)
        str_data = re.sub(r'[\w]{64}', 'token', str_data)

        return json.loads(str_data)

    def assertCalls(self, actual, expected, msg=None):
        calls = []
        for (args, kwargs) in self.normalize(actual):
            calls.append((args, kwargs))

        self.assertListEqual(self.normalize(calls), expected, msg)

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def setUp(self, *args):
        super(AwsTestCase, self).setUp()

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
        self._config_id = str(r.json['_id'])
        # Create EC2 cluster
        body = {
            'config': [
                {
                    '_id': self._config_id
                }
            ],
            'name': 'testing',
            'template': 'default_cluster'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        self._cluster_id = str(r.json['_id'])
        self._cluster_config_id = str(r.json['config']['_id'])


    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    def test_create(self, EasyEC2):

        instance = EasyEC2.return_value
        instance.get_region.side_effect = EC2ResponseError(401, '', '')

        body = {
            'name': 'myprof',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            #'regionHost': 'cornwall.ec2.amazon.com',
            'availabilityZone': 'cornwall-2b'
        }

        # Check we handle invalid credentials
        create_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)

        # Check we handle invalid region
        instance.get_region.side_effect = RegionDoesNotExist('')
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)

        # Check we handle invalid zone
        instance.get_region.side_effect = ZoneDoesNotExist('', '')
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)


        instance.get_region.side_effect = None
        instance.get_region.return_value = EasyDict({'endpoint': 'cornwall.ec2.amazon.com'})

        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        profile_id = str(r.json['_id'])

        expected = {
            u'availabilityZone': u'cornwall-2b',
            u'name': u'myprof',
            u'regionHost': u'cornwall.ec2.amazon.com',
            u'accessKeyId': u'mykeyId',
            u'secretAccessKey': u'mysecret',
            u'regionName': u'cornwall'
        }

        profile = self.model('aws', 'cumulus').load(profile_id, force=True)
        del profile['_id']
        del profile['access']
        del profile['userId']
        self.assertEqual(profile, expected, 'User aws property not updated as expected')

        # Try create another one with the same name
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)

    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    def test_update(self, EasyEC2):
        region_host = 'cornwall.ec2.amazon.com'
        instance = EasyEC2.return_value
        instance.get_region.return_value = EasyDict({'endpoint': region_host})

        profile_name = 'myprof'
        body = {
            'name': profile_name,
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

        change_value = 'change'
        body = {
            'accessKeyId': change_value,
        }

        update_url = '/user/%s/aws/profiles/%s' % (str(self._user['_id']), profile_id)
        r = self.request(update_url, method='PATCH',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatusOk(r)

        profile = self.model('aws', 'cumulus').load(profile_id, user=self._user)
        self.assertEqual(profile['accessKeyId'], change_value, 'Value was not changed')

        # Try changing profile that doesn't exist
        update_url = '/user/%s/aws/profiles/%s' % (str(self._user['_id']), 'bogus')
        r = self.request(update_url, method='PATCH',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)

        # Try changing all values
        change_value = 'cchange ...'
        body = {
            'accessKeyId': change_value,
            'secretAccessKey': change_value,
            'regionName': change_value,
            'regionHost': region_host,
            'availabilityZone': change_value
        }
        update_url = '/user/%s/aws/profiles/%s' % (str(self._user['_id']), profile_id)
        r = self.request(update_url, method='PATCH',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatusOk(r)
        profile = self.model('aws', 'cumulus').load(profile_id, user=self._user)
        del profile['name']
        del profile['access']
        del profile['userId']
        del profile['_id']

        self.assertEqual(profile, body, 'Profile values not updated')


    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    def test_delete(self, EasyEC2):
        region_host = 'cornwall.ec2.amazon.com'
        instance = EasyEC2.return_value
        instance.get_region.return_value = EasyDict({'endpoint': region_host})

        body = {
            'name': 'myprof',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            'regionHost': 'cornwall.ec2.amazon.com',
            'availabilityZone': 'cornwall-2b'
        }

        create_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        profile_id =  str(r.json['_id'])

        delete_url = '/user/%s/aws/profiles/%s' % (str(self._user['_id']), profile_id)
        r = self.request(delete_url, method='DELETE', user=self._user)
        self.assertStatusOk(r)

        profile = self.model('aws', 'cumulus').load(profile_id)

        self.assertFalse(profile, 'Expect profiles to be empty')

    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    def test_get(self, EasyEC2):
        region_host = 'cornwall.ec2.amazon.com'
        instance = EasyEC2.return_value
        instance.get_region.return_value = EasyDict({'endpoint': region_host})

        profile1 = {
            'name': 'myprof1',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            'regionHost': 'cornwall.ec2.amazon.com',
            'availabilityZone': 'cornwall-2b'
        }

        create_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(profile1),
                         user=self._user)
        self.assertStatus(r, 201)
        profile1_id = r.json['_id']

        profile2 = {
            'name': 'myprof2',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            'regionHost': 'cornwall.ec2.amazon.com',
            'availabilityZone': 'cornwall-2b'
        }

        create_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(profile2),
                         user=self._user)
        self.assertStatus(r, 201)
        profile2_id = r.json['_id']


        get_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        r = self.request(get_url, method='GET', user=self._user)
        self.assertStatusOk(r)

        # Assert that we don't get the secret back!
        del profile1['secretAccessKey']
        del profile2['secretAccessKey']
        profile1['_id'] = profile1_id
        profile2['_id'] = profile2_id

        self.assertEqual(r.json, [profile1, profile2], 'Check profiles where returned')


