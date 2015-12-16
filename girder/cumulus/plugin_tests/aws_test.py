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
import mock
import re
import tempfile
from easydict import EasyDict
from boto.exception import EC2ResponseError
from starcluster.exception import RegionDoesNotExist, ZoneDoesNotExist


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
        self.config = {u'permission': [{u'http': {u'to_port': u'80', u'from_port': u'80', u'ip_protocol': u'tcp'}}, {u'http8080': {u'to_port': u'8080', u'from_port': u'8080', u'ip_protocol': u'tcp'}}, {u'https': {u'to_port': u'443', u'from_port': u'443', u'ip_protocol': u'tcp'}}, {u'paraview': {u'to_port': u'11111', u'from_port': u'11111', u'ip_protocol': u'tcp'}}, {u'ssh': {u'to_port': u'22', u'from_port': u'22', u'ip_protocol': u'tcp'}}], u'global': {u'default_template': u''}, u'aws': [{u'info': {u'aws_secret_access_key': u'3z/PSglaGt1MGtGJ', u'aws_region_name': u'us-west-2', u'aws_region_host': u'ec2.us-west-2.amazonaws.com', u'aws_access_key_id': u'AKRWOVFSYTVQ2Q', u'aws_user_id': u'cjh'}}], u'cluster': [
            {u'default_cluster': {u'availability_zone': u'us-west-2a', u'master_instance_type': u't1.micro', u'node_image_id': u'ami-b2badb82', u'cluster_user': u'ubuntu', u'public_ips': u'True', u'keyname': u'cjh', u'cluster_size': u'2', u'plugins': u'requests-installer', u'node_instance_type': u't1.micro', u'permissions': u'ssh, http, paraview, http8080'}}], u'key': [{u'cjh': {u'key_location': u'/home/cjh/work/source/cumulus/cjh.pem'}}], u'plugin': [{u'requests-installer': {u'setup_class': u'starcluster.plugins.pypkginstaller.PyPkgInstaller', u'packages': u'requests, requests-toolbelt'}}]}

        config_body = {
            'name': 'test',
            'config': self.config
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
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    def test_create(self, generate_key_pair, EasyEC2):

        instance = EasyEC2.return_value
        instance.get_region.side_effect = EC2ResponseError(401, '', '')

        body = {
            'name': 'myprof',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
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
            u'regionName': u'cornwall',
            u'status': 'creating',
            u'publicIPs': False
        }

        profile = self.model('aws', 'cumulus').load(profile_id, force=True)
        del profile['_id']
        del profile['access']
        del profile['userId']
        self.assertEqual(profile, expected, 'User aws property not updated as expected')

        # Check that we fired of a task to create the key pair for this profile
        self.assertEqual(len(generate_key_pair.call_args_list), 1, 'Task to create key not called')

        # Try create another one with the same name
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)

    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    def test_create_public_ips(self, generate_key_pair, EasyEC2):

        instance = EasyEC2.return_value

        body = {
            'name': 'myprof',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            'availabilityZone': 'cornwall-2b',
            'publicIPs': 'True'
        }

        # Check we handle invalid credentials
        create_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        instance.get_region.return_value = EasyDict({'endpoint': 'cornwall.ec2.amazon.com'})

        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)
        body['publicIPs'] = True

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
            u'regionName': u'cornwall',
            u'status': 'creating',
            u'publicIPs': True
        }

        profile = self.model('aws', 'cumulus').load(profile_id, force=True)
        del profile['_id']
        del profile['access']
        del profile['userId']
        self.assertEqual(profile, expected, 'User aws property not updated as expected')


    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    def test_update(self, EasyEC2, delay):
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

        # Try changing all values ( only the key related one should change )
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
        del profile['access']
        del profile['userId']
        del profile['_id']

        expected = {
            'name': profile_name,
            'accessKeyId': change_value,
            'secretAccessKey': change_value,
            'regionName': 'cornwall',
            'regionHost': region_host,
            'availabilityZone': 'cornwall-2b',
            'status': 'creating',
            'publicIPs': False
        }

        self.assertEqual(profile, expected, 'Profile values not updated')

        # Try updating errorMessage
        body = {
            'accessKeyId': change_value,
            'secretAccessKey': change_value,
            'regionName': change_value,
            'regionHost': region_host,
            'availabilityZone': change_value,
            'status': 'error',
            'errorMessage': 'some message'
        }
        update_url = '/user/%s/aws/profiles/%s' % (str(self._user['_id']), profile_id)
        r = self.request(update_url, method='PATCH',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatusOk(r)
        profile = self.model('aws', 'cumulus').load(profile_id, user=self._user)
        del profile['access']
        del profile['userId']
        del profile['_id']

        expected = {
            u'status': u'error',
            u'availabilityZone': u'cornwall-2b',
            u'name': u'myprof',
            u'regionHost': u'cornwall.ec2.amazon.com',
            u'errorMessage': u'some message',
            u'accessKeyId': u'cchange ...',
            u'secretAccessKey': u'cchange ...',
            u'regionName': u'cornwall',
            u'publicIPs': False
        }
        self.assertEqual(profile, expected, 'Profile values not updated')

        # Check we get the right server side events
        r = self.request('/notification/stream', method='GET', user=self._user,
                         isJson=False, params={'timeout': 0})
        self.assertStatusOk(r)
        notifications = self.getSseMessages(r)
        self.assertEqual(len(notifications), 1, 'Expecting a single notification')
        notification = notifications[0]
        notification_type = notification['type']
        data = notification['data']
        self.assertEqual(notification_type, 'profile.status')
        expected = {
            u'status': u'error',
            u'_id': profile_id
        }
        self.assertEqual(data, expected, 'Unexpected notification data')

        # Test update public ips
        body = {
            'publicIPs': True
        }
        update_url = '/user/%s/aws/profiles/%s' % (str(self._user['_id']), profile_id)
        r = self.request(update_url, method='PATCH',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatusOk(r)
        profile = self.model('aws', 'cumulus').load(profile_id, user=self._user)
        del profile['access']
        del profile['userId']
        del profile['_id']

        expected = {
            u'status': u'error',
            u'availabilityZone': u'cornwall-2b',
            u'name': u'myprof',
            u'regionHost': u'cornwall.ec2.amazon.com',
            u'errorMessage': u'some message',
            u'accessKeyId': u'cchange ...',
            u'secretAccessKey': u'cchange ...',
            u'regionName': u'cornwall',
            u'publicIPs': True
        }
        self.assertEqual(profile, expected, 'Profile values not updated')



    @mock.patch('girder.plugins.cumulus.volume.get_easy_ec2')
    @mock.patch('girder.plugins.cumulus.aws.get_easy_ec2')
    @mock.patch('cumulus.aws.ec2.tasks.key.delete_key_pair.delay')
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay' )
    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    def test_delete(self, EasyEC2, generate_key_pair, delete_key_pair,
                    get_easy_ec2, volume_get_easy_ec2):
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

        # Create and new profile and associate it with a configuration, this
        # should prevent it from being deleted
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
        profile_id = str(r.json['_id'])

        config_body = {
            'name': 'profile_test',
            'config': self.config,
            'aws': {
                'profileId': profile_id
            }
        }

        r = self.request('/starcluster-configs', method='POST',
                         type='application/json', body=json.dumps(config_body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        config_id = str(r.json['_id'])

        delete_url = '/user/%s/aws/profiles/%s' % (str(self._user['_id']), profile_id)
        r = self.request(delete_url, method='DELETE', user=self._user)
        self.assertStatus(r, 400)

        # Now delete the config
        r = self.request('/starcluster-configs/%s' % config_id, method='DELETE',
                         type='application/json', body=json.dumps(config_body),
                         user=self._cumulus)
        self.assertStatusOk(r)

        r = self.request(delete_url, method='DELETE', user=self._user)
        self.assertStatusOk(r)

        # Create and new profile and associate it with a volume, this
        # should prevent it from being deleted
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
        profile_id = str(r.json['_id'])

        volume_id = 'vol-1'
        volume_get_easy_ec2.return_value.create_volume.return_value.id \
            = volume_id

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'aws': {
                'profileId': profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        volume_id = str(r.json['_id'])

        delete_url = '/user/%s/aws/profiles/%s' % (str(self._user['_id']), profile_id)
        r = self.request(delete_url, method='DELETE', user=self._user)
        self.assertStatus(r, 400)

        # Now delete the volume
        r = self.request('/volumes/%s' % volume_id, method='DELETE',
                 type='application/json', body=json.dumps(body),
                 user=self._user)
        self.assertStatusOk(r)

        r = self.request(delete_url, method='DELETE', user=self._user)
        self.assertStatusOk(r)

    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    def test_get(self, EasyEC2, generate_key_pair):
        region_host = 'cornwall.ec2.amazon.com'
        instance = EasyEC2.return_value
        instance.get_region.return_value = EasyDict({'endpoint': region_host})

        profile1 = {
            'name': 'myprof1',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            'regionHost': 'cornwall.ec2.amazon.com',
            'availabilityZone': 'cornwall-2b',
            'status': 'creating',
            'publicIPs': False
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
            'availabilityZone': 'cornwall-2b',
            'status': 'creating',
            'publicIPs': False
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

    @mock.patch('girder.plugins.cumulus.aws.get_easy_ec2')
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    def test_running_instances(self, EasyEC2, generate_key_pair, get_easy_ec2):
        region_host = 'cornwall.ec2.amazon.com'
        instance = EasyEC2.return_value
        instance.get_region.return_value = EasyDict({'endpoint': region_host})

        profile = {
            'name': 'myprof1',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            'regionHost': 'cornwall.ec2.amazon.com',
            'availabilityZone': 'cornwall-2b',
            'status': 'creating',
            'publicIPs': False
        }

        create_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(profile),
                         user=self._user)
        self.assertStatus(r, 201)
        profile_id = r.json['_id']

        get_easy_ec2.return_value.get_running_instance_count.return_value = 10

        running_instances_url = '/user/%s/aws/profiles/%s/runninginstances' % \
            (str(self._user['_id']), str(profile_id))
        r = self.request(running_instances_url, method='GET', user=self._user)
        self.assertStatusOk(r)
        expected = {
            u'runninginstances': 10
        }
        self.assertEqual(expected, r.json, 'Unexpected response')

    @mock.patch('girder.plugins.cumulus.aws.get_easy_ec2')
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('girder.plugins.cumulus.models.aws.EasyEC2')
    def test_max_instances(self, EasyEC2, generate_key_pair, get_easy_ec2):
        region_host = 'cornwall.ec2.amazon.com'
        instance = EasyEC2.return_value
        instance.get_region.return_value = EasyDict({'endpoint': region_host})

        profile = {
            'name': 'myprof1',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            'regionHost': 'cornwall.ec2.amazon.com',
            'availabilityZone': 'cornwall-2b',
            'status': 'creating',
            'publicIPs': False
        }

        create_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(profile),
                         user=self._user)
        self.assertStatus(r, 201)
        profile_id = r.json['_id']

        get_easy_ec2.return_value.get_max_instances.return_value = 100

        running_instances_url = '/user/%s/aws/profiles/%s/maxinstances' % \
            (str(self._user['_id']), str(profile_id))
        r = self.request(running_instances_url, method='GET', user=self._user)
        self.assertStatusOk(r)
        expected = {
            u'maxinstances': 100
        }
        self.assertEqual(expected, r.json, 'Unexpected response')


