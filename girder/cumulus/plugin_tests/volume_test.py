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
from easydict import EasyDict

def setUpModule():
    base.enabledPlugins.append('cumulus')
    base.startServer()


def tearDownModule():
    base.stopServer()


class VolumeTestCase(base.TestCase):

    def normalize(self, data):
        str_data = json.dumps(data, default=str)
        str_data = re.sub(r'[\w]{64}', 'token', str_data)

        return json.loads(str_data)

    def assertCalls(self, actual, expected, msg=None):
        calls = []
        for (args, kwargs) in self.normalize(actual):
            calls.append((args, kwargs))

        self.assertListEqual(self.normalize(calls), expected, msg)

    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    @mock.patch('girder.plugins.cumulus.models.aws.get_ec2_client')
    def setUp(self, get_ec2_client, *args):
        super(VolumeTestCase, self).setUp()

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

        # Create a traditional cluster
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
        self._trad_cluster_id = str(r.json['_id'])

        with open('plugins/cumulus/plugin_tests/fixtures/test.ini') as fp:
            self._ini_file = fp.readlines()

        self._ini_file.append('')

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

        # Create a AWS profile
        self._availability_zone = 'cornwall-2b'
        body = {
            'name': 'myprof',
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

        create_url = '/user/%s/aws/profiles' % str(self._another_user['_id'])
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._another_user)
        self.assertStatus(r, 201)
        self._another_profile_id = str(r.json['_id'])

    @mock.patch('girder.plugins.cumulus.volume.get_ec2_client')
    def test_create(self, get_ec2_client):
        volume_id = 'vol-1'
        ec2_client = get_ec2_client.return_value
        ec2_client.create_volume.return_value = {
            'VolumeId': volume_id
        }

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'aws': {
                'profileId': self._profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        expected = {
            u'name': u'test',
            u'zone': u'us-west-2a',
            u'type': u'ebs',
            u'size': 20,
            u'ec2': {
                u'id': volume_id
            },
            u'aws': {
                u'profileId': self._profile_id
            }
        }
        del r.json['_id']
        self.assertEqual(r.json, expected, 'Unexpected volume returned')

        # Try invalid type
        body['type'] = 'bogus'
        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 400)
        # Add file system type
        body = {
            'name': 'test2',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'fs': 'ext4',
            'aws': {
                'profileId': self._profile_id
            }
        }
        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        expected = {
            u'name': u'test2',
            u'zone': u'us-west-2a',
            u'type': u'ebs',
            u'size': 20,
            u'fs': u'ext4',
            u'ec2': {
                u'id': volume_id
            },
            u'aws': {
                u'profileId': self._profile_id
            }
        }
        del r.json['_id']

        self.assertEqual(r.json, expected, 'Unexpected volume returned')
        # Try invalid file system type
        body['fs'] = 'bogus'
        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 400)

        # Try create volume with same name
        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'aws': {
                'profileId': self._profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)

        # Now try create volume with same name another user this should work
        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'aws': {
                'profileId': self._profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 201)

        # Create a volume without a zone
        body = {
            'name': 'zoneless',
            'size': 20,
            'type': 'ebs',
            'aws': {
                'profileId': self._profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        self.assertEqual(r.json['zone'], self._availability_zone,
                         'Volume created in wrong zone')

        # Try to create a volume with a invalid profile
        body['aws'] = {
            'profileId': 'bogus'
        }
        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 400)

    @mock.patch('girder.plugins.cumulus.volume.get_ec2_client')
    def test_get(self, get_ec2_client):
        volume_id = 'vol-1'
        ec2_client = get_ec2_client.return_value
        ec2_client.create_volume.return_value = {
            'VolumeId': volume_id
        }

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'aws': {
                'profileId': self._profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        volume_id = str(r.json['_id'])

        expected = {
            u'name': u'test',
            u'zone': u'us-west-2a',
            u'ec2': {
                u'id': u'vol-1'
            },
            u'type':
            u'ebs',
            u'size': 20,
            u'aws': {
                u'profileId': self._profile_id
            }
        }

        r = self.request('/volumes/%s' % volume_id, method='GET',
                         type='application/json',
                         user=self._cumulus)
        self.assertStatusOk(r)
        del r.json['_id']
        self.assertEqual(expected, r.json)

        # Try to fetch a volume that doesn't exist
        r = self.request('/volumes/55c3dbd9f65710591baefe60', method='GET',
                         type='application/json',
                         user=self._cumulus)
        self.assertStatus(r, 400)

    @mock.patch('girder.plugins.cumulus.volume.get_ec2_client')
    def test_delete(self, get_ec2_client):
        volume_id = 'vol-1'
        ec2_client = get_ec2_client.return_value
        ec2_client.create_volume.return_value = {
            'VolumeId': volume_id
        }

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'aws': {
                'profileId': self._profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        volume = r.json
        volume_id = str(r.json['_id'])

        # Try and delete any attached volume
        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'available'

            }]
        }
        body = {
            'path': '/data'
        }

        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatusOk(r)

        r = self.request('/volumes/%s' % volume_id, method='DELETE',
                         user=self._cumulus)
        self.assertStatus(r, 400)

        # Detach it then delete it
        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'in-use'

            }]
        }

        url = '/volumes/%s/detach' % (volume_id)
        r = self.request(url, method='PUT', user=self._cumulus)
        self.assertStatusOk(r)

        r = self.request('/volumes/%s' % volume_id, method='DELETE',
                         user=self._cumulus)
        self.assertStatus(r, 200)



    @mock.patch('girder.plugins.cumulus.volume.get_ec2_client')
    def test_attach_volume(self, get_ec2_client):

        ec2_volume_id = 'vol-1'
        ec2_client = get_ec2_client.return_value
        ec2_client.create_volume.return_value = {
            'VolumeId': ec2_volume_id
        }

        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'available'

            }]
        }
        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'fs': 'ext4',
            'aws': {
                'profileId': self._profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        volume_id = str(r.json['_id'])

        body = {
            'path': '/data'
        }

        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)

        self.assertStatusOk(r)

        r = self.request('/starcluster-configs/%s' % self._cluster_config_id, method='GET',
                         type='application/json', user=self._cumulus)
        self.assertStatusOk(r)
        expected = {u'plugin': [{u'requests-installer': {u'setup_class': u'starcluster.plugins.pypkginstaller.PyPkgInstaller', u'packages': u'requests, requests-toolbelt'}}], u'vol': [{u'test': {u'fs': u'ext4', u'mount_path': u'/data', u'volume_id': u'vol-1'}}], u'global': {u'default_template': u''}, u'aws': [{u'info': {u'aws_user_id': u'cjh', u'aws_region_name': u'us-west-2', u'aws_region_host': u'ec2.us-west-2.amazonaws.com', u'aws_access_key_id': u'AKRWOVFSYTVQ2Q', u'aws_secret_access_key': u'3z/PSglaGt1MGtGJ'}}], u'cluster': [{u'default_cluster': {u'plugins': u'requests-installer', u'volumes': u'test', u'availability_zone': u'us-west-2a', u'master_instance_type': u't1.micro', u'cluster_user': u'ubuntu',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              u'public_ips': u'True', u'keyname': u'cjh', u'cluster_size': u'2', u'node_image_id': u'ami-b2badb82', u'node_instance_type': u't1.micro', u'permissions': u'ssh, http, paraview, http8080'}}], u'key': [{u'cjh': {u'key_location': u'/home/cjh/work/source/cumulus/cjh.pem'}}], u'permission': [{u'http': {u'to_port': u'80', u'from_port': u'80', u'ip_protocol': u'tcp'}}, {u'http8080': {u'to_port': u'8080', u'from_port': u'8080', u'ip_protocol': u'tcp'}}, {u'https': {u'to_port': u'443', u'from_port': u'443', u'ip_protocol': u'tcp'}}, {u'paraview': {u'to_port': u'11111', u'from_port': u'11111', u'ip_protocol': u'tcp'}}, {u'ssh': {u'to_port': u'22', u'from_port': u'22', u'ip_protocol': u'tcp'}}]}
        self.assertEqual(
            expected, r.json, 'Config was not successfully updated')

        # Try to attach volume that is already attached
        ec2_client.reset_mock()
        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'in-use'

            }]
        }

        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 400)

        # Try to attach volume that is currently being created
        ec2_client.reset_mock()
        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'creating'

            }]
        }

        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)

        self.assertStatus(r, 400)
        # Try to attach volume to traditional cluster
        ec2_client.reset_mock()
        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'creating'

            }]
        }

        url = '/volumes/%s/clusters/%s/attach' % (
            volume_id, self._trad_cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 400)

    @mock.patch('girder.plugins.cumulus.volume.get_ec2_client')
    def test_detach_volume(self, get_ec2_client):
        ec2_volume_id = 'vol-1'
        ec2_client = get_ec2_client.return_value
        ec2_client.create_volume.return_value = {
            'VolumeId': ec2_volume_id
        }
        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'available'
            }]
        }

        body = {
            'name': 'testing me',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'aws': {
                'profileId': self._profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        volume_id = str(r.json['_id'])

        # Try detaching volume not in use
        url = '/volumes/%s/detach' % (volume_id)
        r = self.request(url, method='PUT', user=self._user)
        self.assertStatus(r, 400)

        body = {
            'path': '/data'
        }

        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatusOk(r)

        # Try successful detach
        ec2_client.reset_mock()
        ec2_client.create_volume.return_value = {
            'VolumeId': ec2_volume_id
        }
        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'in-use'
            }]
        }

        url = '/volumes/%s/detach' % (volume_id)
        r = self.request(url, method='PUT', user=self._user)
        self.assertStatusOk(r)

        # Assert that detach was called on ec2 object
        self.assertEqual(len(ec2_client.detach_volume.call_args_list),
                         1, "detach was not called")

        r = self.request('/starcluster-configs/%s' % self._cluster_config_id, method='GET',
                         type='application/json', user=self._cumulus)
        expected = {u'plugin': [{u'requests-installer': {u'setup_class': u'starcluster.plugins.pypkginstaller.PyPkgInstaller', u'packages': u'requests, requests-toolbelt'}}], u'vol': [], u'global': {u'default_template': u''}, u'aws': [{u'info': {u'aws_user_id': u'cjh', u'aws_region_name': u'us-west-2', u'aws_region_host': u'ec2.us-west-2.amazonaws.com', u'aws_access_key_id': u'AKRWOVFSYTVQ2Q', u'aws_secret_access_key': u'3z/PSglaGt1MGtGJ'}}], u'cluster': [{u'default_cluster': {u'plugins': u'requests-installer', u'availability_zone': u'us-west-2a', u'master_instance_type': u't1.micro', u'cluster_user': u'ubuntu', u'public_ips': u'True', u'keyname': u'cjh',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  u'cluster_size': u'2', u'node_image_id': u'ami-b2badb82', u'node_instance_type': u't1.micro', u'permissions': u'ssh, http, paraview, http8080'}}], u'key': [{u'cjh': {u'key_location': u'/home/cjh/work/source/cumulus/cjh.pem'}}], u'permission': [{u'http': {u'to_port': u'80', u'from_port': u'80', u'ip_protocol': u'tcp'}}, {u'http8080': {u'to_port': u'8080', u'from_port': u'8080', u'ip_protocol': u'tcp'}}, {u'https': {u'to_port': u'443', u'from_port': u'443', u'ip_protocol': u'tcp'}}, {u'paraview': {u'to_port': u'11111', u'from_port': u'11111', u'ip_protocol': u'tcp'}}, {u'ssh': {u'to_port': u'22', u'from_port': u'22', u'ip_protocol': u'tcp'}}]}
        self.assertStatusOk(r)
        self.assertEqual(expected, r.json, 'Config was not updated correctly')

    @mock.patch('girder.plugins.cumulus.volume.get_ec2_client')
    def test_find_volume(self, get_ec2_client):
        ec2_volume_id = 'vol-1'
        ec2_client = get_ec2_client.return_value
        ec2_client.create_volume.return_value = {
            'VolumeId': ec2_volume_id
        }
        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'available'
            }]
        }

        # Create some test volumes
        body = {
            'name': 'testing me',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'aws': {
                'profileId': self._profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        volume_1_id = r.json['_id']

        body = {
            'name': 'testing me2',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'aws': {
                'profileId': self._another_profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._another_user)
        self.assertStatus(r, 201)
        volume_2_id = r.json['_id']

        # Search with one user
        r = self.request('/volumes', method='GET', user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 1, 'Wrong number of volumes returned')
        self.assertEqual(r.json[0]['_id'], volume_1_id, 'Wrong volume returned')

        # Now search with the other
        r = self.request('/volumes', method='GET', user=self._another_user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 1, 'Wrong number of volumes returned')
        self.assertEqual(r.json[0]['_id'], volume_2_id, 'Wrong volume returned')

        # Seach for volumes attached to a particular cluster
        params = {
            'clusterId': self._cluster_id
        }
        r = self.request('/volumes', method='GET', user=self._user,
                         params=params)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 0, 'Wrong number of volumes returned')

        body = {
            'path': '/data'
        }

        # Attach a volume
        url = '/volumes/%s/clusters/%s/attach' % (str(volume_1_id), self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatusOk(r)

        # Search again
        r = self.request('/volumes', method='GET', user=self._user,
                         params=params)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 1, 'Wrong number of volumes returned')

    @mock.patch('girder.plugins.cumulus.volume.get_ec2_client')
    def test_get_status(self, get_ec2_client):
        ec2_volume_id = 'vol-1'
        ec2_client = get_ec2_client.return_value
        ec2_client.create_volume.return_value = {
            'VolumeId': ec2_volume_id
        }
        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'available'
            }]
        }
        # Create some test volumes
        body = {
            'name': 'testing me',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'aws': {
                'profileId': self._profile_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        volume_id = str(r.json['_id'])

        # Get status
        url = '/volumes/%s/status' % volume_id
        r = self.request(url, method='GET', user=self._user)
        self.assertStatusOk(r)
        expected = {
            u'status': u'available'
        }
        self.assertEqual(r.json, expected, 'Unexpected status')

        ec2_client.describe_volumes.return_value = {
            'Volumes': [{
                'State': 'in-use'
            }]
        }
        r = self.request(url, method='GET', user=self._user)
        self.assertStatusOk(r)
        expected = {
            u'status': u'in-use'
        }
        self.assertEqual(r.json, expected, 'Unexpected status')
