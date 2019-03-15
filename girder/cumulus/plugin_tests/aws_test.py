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
from botocore.exceptions import ClientError
from cumulus.aws.ec2 import ClientErrorCode
from cumulus.testing import AssertCallsMixin
from cumulus.constants import VolumeState

from girder.utility.model_importer import ModelImporter

def setUpModule():
    base.enabledPlugins.append('cumulus')
    base.startServer()


def tearDownModule():
    base.stopServer()


class AwsTestCase(AssertCallsMixin, base.TestCase):
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus_plugin.models.aws.get_ec2_client')
    def setUp(self, get_ec2_client, *args):
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
            [ModelImporter.model('user').createUser(**user) for user in users]

        self._group = ModelImporter.model('group').createGroup('cumulus', self._cumulus)

        # Create a AWS profile
        self._availability_zone = 'cornwall-2b'
        body = {
            'name': 'setup',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            'availabilityZone': self._availability_zone
        }

        ec2_client = get_ec2_client.return_value
        ec2_client.describe_regions.return_value = {
            'Regions': [{
                'RegionName': 'cornwall',
                'Endpoint': 'cornwall.ec2.amazon.com'
                }]
        }

        create_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        self._profile_id = str(r.json['_id'])


        # Create EC2 cluster
        body = {
            'name': 'testing',
            'profileId': self._profile_id
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        self._cluster_id = str(r.json['_id'])

    @mock.patch('cumulus_plugin.models.aws.get_ec2_client')
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    def test_create(self, generate_key_pair, get_ec2_client):
        ec2_client = get_ec2_client.return_value
        response = {
            'Error': {
                'Code': ClientErrorCode.AuthFailure
            }
        }
        get_ec2_client.side_effect = ClientError(response, '')

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
        get_ec2_client.side_effect = None
        response = {
            'Error': {
                'Code': ClientErrorCode.InvalidParameterValue
            }
        }
        ec2_client.describe_regions.side_effect = ClientError(response, '')

        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)

        # Check we handle invalid zone
        response = {
            'Error': {
                'Code': ClientErrorCode.InvalidParameterValue
            }
        }
        ec2_client.describe_availability_zones.side_effect = ClientError(response, '')
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)

        ec2_client.describe_availability_zones.side_effect = None
        ec2_client.describe_regions.side_effect = None
        ec2_client.describe_regions.return_value = {
            'Regions': [{
                'RegionName': 'cornwall',
                'Endpoint': 'cornwall.ec2.amazon.com'
            }]
        }

        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        profile_id = str(r.json['_id'])

        expected = {
            u'availabilityZone': u'cornwall-2b',
            u'name': u'myprof',
            u'cloudProvider': u'ec2',
            u'regionHost': u'cornwall.ec2.amazon.com',
            u'accessKeyId': u'mykeyId',
            u'secretAccessKey': u'mysecret',
            u'regionName': u'cornwall',
            u'status': 'creating',
            u'publicIPs': False
        }

        profile = ModelImporter.model('aws', 'cumulus').load(profile_id, force=True)
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

    @mock.patch('cumulus_plugin.models.aws.get_ec2_client')
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    def test_create_public_ips(self, generate_key_pair, get_ec2_client):

        ec2_client = get_ec2_client.return_value

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
        ec2_client.describe_regions.return_value = {
            'Regions': [{
                'RegionName': 'cornwall',
                'Endpoint': 'cornwall.ec2.amazon.com'
            }]
        }

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
            u'cloudProvider': u'ec2',
            u'regionHost': u'cornwall.ec2.amazon.com',
            u'accessKeyId': u'mykeyId',
            u'secretAccessKey': u'mysecret',
            u'regionName': u'cornwall',
            u'status': 'creating',
            u'publicIPs': True
        }

        profile = ModelImporter.model('aws', 'cumulus').load(profile_id, force=True)
        del profile['_id']
        del profile['access']
        del profile['userId']
        self.assertEqual(profile, expected, 'User aws property not updated as expected')


    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus_plugin.models.aws.get_ec2_client')
    def test_update(self, get_ec2_client, delay):
        region_host = 'cornwall.ec2.amazon.com'
        ec2_client = get_ec2_client.return_value
        ec2_client.describe_regions.return_value = {
            'Regions': [{
                'RegionName': 'cornwall',
                'Endpoint': region_host
            }]
        }

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

        profile = ModelImporter.model('aws', 'cumulus').load(profile_id, user=self._user)
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
        profile = ModelImporter.model('aws', 'cumulus').load(profile_id, user=self._user)
        del profile['access']
        del profile['userId']
        del profile['_id']

        expected = {
            'name': profile_name,
            'cloudProvider': 'ec2',
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
        profile = ModelImporter.model('aws', 'cumulus').load(profile_id, user=self._user)
        del profile['access']
        del profile['userId']
        del profile['_id']

        expected = {
            u'status': u'error',
            u'availabilityZone': u'cornwall-2b',
            u'name': u'myprof',
            u'cloudProvider': u'ec2',
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
        self.assertEqual(len(notifications), 2, 'Expecting a two notifications')
        notification = notifications[1]
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
        profile = ModelImporter.model('aws', 'cumulus').load(profile_id, user=self._user)
        del profile['access']
        del profile['userId']
        del profile['_id']

        expected = {
            u'status': u'error',
            u'availabilityZone': u'cornwall-2b',
            u'name': u'myprof',
            u'cloudProvider': u'ec2',
            u'regionHost': u'cornwall.ec2.amazon.com',
            u'errorMessage': u'some message',
            u'accessKeyId': u'cchange ...',
            u'secretAccessKey': u'cchange ...',
            u'regionName': u'cornwall',
            u'publicIPs': True
        }
        self.assertEqual(profile, expected, 'Profile values not updated')

    @mock.patch('cumulus.ansible.tasks.volume.delete_volume.delay')
    @mock.patch('cumulus_plugin.aws.get_ec2_client')
    @mock.patch('cumulus.aws.ec2.tasks.key.delete_key_pair.delay')
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus_plugin.models.aws.get_ec2_client')
    def test_delete(self, get_ec2_client, generate_key_pair, delete_key_pair,
                    aws_get_ec2_client, delete_volume_delay):
        region_host = 'cornwall.ec2.amazon.com'
        ec2_client = get_ec2_client.return_value
        ec2_client.describe_regions.return_value = {
            'Regions': [{
                'RegionName': 'cornwall',
                'Endpoint': region_host
            }]
        }

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

        profile = ModelImporter.model('aws', 'cumulus').load(profile_id)

        self.assertFalse(profile, 'Expect profiles to be empty')

        # Create and new profile and associate it with a cluster, this
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
        profile = r.json
        profile_id = str(r.json['_id'])

        cluster_body = {
            'name': 'profile_test',
            'profileId': profile_id,
            'config': {
                'launch': {
                    'params': {
                        'ansible_ssh_user': 'ubuntu'
                    }
                }
            }
        }

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json.dumps(cluster_body),
                         user=self._user)
        self.assertStatus(r, 201)
        cluster_id = str(r.json['_id'])

        # Now delete the cluster
        r = self.request('/clusters/%s' % cluster_id, method='DELETE',
                         type='application/json',
                         user=self._user)
        self.assertStatusOk(r)

        delete_url = '/user/%s/aws/profiles/%s' % (str(self._user['_id']), profile_id)
        r = self.request(delete_url, method='DELETE', user=self._user)
        self.assertStatusOk(r)

        # Create a new profile and associate it with a volume, this
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

        body = {
            "name": "some_volume",
            "profileId": profile_id,
            "size": 17,
            "type": "ebs",
            "zone": "us-west-2a"
        }

        create_volume_url = '/volumes'
        r = self.request(create_volume_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        volume_id = str(r.json['_id'])

        # Patch the volume so it looks available, normally ansible script does this
        body = {
            'status': 'available',
            'ec2': {
                'id': '0123456789'
            }
        }
        patch_volume_url = '/volumes/%s' % volume_id
        r = self.request(patch_volume_url, method='PATCH',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatusOk(r)

        delete_url = '/user/%s/aws/profiles/%s' % (str(self._user['_id']), profile_id)
        r = self.request(delete_url, method='DELETE', user=self._user)
        self.assertStatus(r, 400)

        # Patch the call to get_volume so we can pass the state test in delete
        with mock.patch('cumulus_plugin.volume.CloudProvider') as provider:
            provider.return_value.get_volume.return_value = {'state': VolumeState.AVAILABLE}
            # delete the volume
            delete_volume_url = '/volumes/%s' % volume_id
            r = self.request(delete_volume_url, method='DELETE', user=self._user)
            self.assertStatusOk(r)

        # delete_volume would normally call delete_complete
        self.assertEquals(delete_volume_delay.call_count, 1)
        delete_volume_complete_url = '/volumes/%s/delete/complete' % volume_id
        r = self.request(delete_volume_complete_url, method='PUT', user=self._user)
        self.assertStatusOk(r)

        # Deleting the profile should now be OK
        r = self.request(delete_url, method='DELETE', user=self._user)
        self.assertStatusOk(r)

    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus_plugin.models.aws.get_ec2_client')
    def test_get(self, get_ec2_client, generate_key_pair):
        region_host = 'cornwall.ec2.amazon.com'
        get_ec2_client = get_ec2_client.return_value
        get_ec2_client.describe_regions.return_value = {
            'Regions': [{
                'RegionName': 'cornwall',
                'Endpoint': region_host
            }]
        }

        profile1 = {
            'name': 'myprof1',
            'cloudProvider': 'ec2',
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
            'cloudProvider': 'ec2',
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

        self.assertEqual(r.json[1:], [profile1, profile2], 'Check profiles were returned')

    @mock.patch('cumulus_plugin.aws.get_ec2_client')
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus_plugin.models.aws.get_ec2_client')
    def test_running_instances(self, get_ec2_client, generate_key_pair, aws_get_ec2_client):
        region_host = 'cornwall.ec2.amazon.com'
        ec2_client = get_ec2_client.return_value
        ec2_client.describe_regions.return_value = {
            'Regions': [{
                'RegionName': 'cornwall',
                'Endpoint': region_host
            }]
        }

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

        aws_get_ec2_client.return_value.running_instances.return_value = 10

        running_instances_url = '/user/%s/aws/profiles/%s/runninginstances' % \
            (str(self._user['_id']), str(profile_id))
        r = self.request(running_instances_url, method='GET', user=self._user)
        self.assertStatusOk(r)
        expected = {
            u'runninginstances': 10
        }
        self.assertEqual(expected, r.json)

    @mock.patch('cumulus_plugin.aws.get_ec2_client')
    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus_plugin.models.aws.get_ec2_client')
    def test_max_instances(self, get_ec2_client, generate_key_pair, aws_get_ec2_client):
        region_host = 'cornwall.ec2.amazon.com'
        ec2_client = get_ec2_client.return_value
        ec2_client.describe_regions.return_value = {
            'Regions': [{
                'RegionName': 'cornwall',
                'Endpoint': region_host
                }]
        }

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

        response = {
            'AccountAttributes': [{
                'AttributeValues': [{
                    'AttributeValue': 100
                }]
            }]
        }
        aws_get_ec2_client.return_value.describe_account_attributes.return_value = response

        running_instances_url = '/user/%s/aws/profiles/%s/maxinstances' % \
            (str(self._user['_id']), str(profile_id))
        r = self.request(running_instances_url, method='GET', user=self._user)
        self.assertStatusOk(r)
        expected = {
            u'maxinstances': 100
        }
        self.assertEqual(expected, r.json, 'Unexpected response')
