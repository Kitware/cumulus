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
from cumulus.testing import AssertCallsMixin
import unittest
from girder.utility.model_importer import ModelImporter

def setUpModule():
    base.enabledPlugins.append('cumulus')
    base.startServer()


def tearDownModule():
    base.stopServer()


class VolumeTestCase(AssertCallsMixin, base.TestCase):

    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus_plugin.models.aws.get_ec2_client')
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
            [ModelImporter.model('user').createUser(**user) for user in users]

        self._group = ModelImporter.model('group').createGroup('cumulus', self._cumulus)

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

        # Create EC2 cluster
        body = {
            'profileId': self._profile_id,
            'name': 'testing',
            'cloudProvider': 'ec2'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        self._cluster_id = str(r.json['_id'])

    def test_create(self):

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'profileId': self._profile_id
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)

        self.assertStatus(r, 201)

        expected = {
            u'status': u'created',
            u'name': u'test',
            u'zone': u'us-west-2a',
            u'ec2': {u'id': None},
            u'profileId': self._profile_id,
            u'type': u'ebs',
            u'size': 20}

        del r.json['_id']
        self.assertEqual(r.json, expected, 'Unexpected volume returned')
        # Add file system type
        body = {
            'name': 'test2',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'fs': 'ext4',
            'profileId': self._profile_id
        }
        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)

        self.assertStatus(r, 201)
        expected = {
            u'status': u'created',
            u'name': u'test2',
            u'zone': u'us-west-2a',
            u'type': u'ebs',
            u'size': 20,
            u'fs': u'ext4',
            u'ec2': {
                u'id': None
            },
            u'profileId': self._profile_id
        }

        del r.json['_id']

        self.assertEqual(r.json, expected, 'Unexpected volume returned')

        # Try invalid type
        body['type'] = 'bogus'
        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)



        # Try invalid file system type
        body['fs'] = 'bogus'
        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)

        # Try create volume with same name
        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'profileId': self._profile_id
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
            'profileId': self._another_profile_id
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._another_user)
        self.assertStatus(r, 201)

        # Create a volume without a zone
        body = {
            'name': 'zoneless',
            'size': 20,
            'type': 'ebs',
            'profileId': self._profile_id
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        self.assertEqual(r.json['zone'], self._availability_zone,
                         'Volume created in wrong zone')

        # Try to create a volume with a invalid profile
        body['aws'] = {
            'profileId': 'bogus'
        }
        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)


    def test_get(self):

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'profileId': self._profile_id
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        volume_id = str(r.json['_id'])

        expected = {
            u'name': u'test',
            u'zone': u'us-west-2a',
            u'ec2': {
                u'id': None
            },
            u'type':
            u'ebs',
            u'size': 20,
            u'status': u'created',
            u'profileId': self._profile_id
        }

        r = self.request('/volumes/%s' % volume_id, method='GET',
                         type='application/json',
                         user=self._user)
        self.assertStatusOk(r)
        del r.json['_id']
        self.assertEqual(expected, r.json)

        # Try to fetch a volume that doesn't exist
        r = self.request('/volumes/55c3dbd9f65710591baefe60', method='GET',
                         type='application/json',
                         user=self._user)
        self.assertStatus(r, 400)

    @mock.patch('cumulus_plugin.volume.CloudProvider')
    @mock.patch('cumulus.ansible.tasks.volume.attach_volume.delay')
    @mock.patch('cumulus.ansible.tasks.volume.detach_volume.delay')
    @mock.patch('cumulus.ansible.tasks.volume.delete_volume.delay')
    def test_delete(self, delete_volume, detach_volume,
                    attach_volume, CloudProvider):

        CloudProvider.return_value.get_volume.return_value = None

        CloudProvider.return_value.get_master_instance.return_value = {
            'instance_id': 'i-00000',
            'private_ip': 'x.x.x.x',
            'public_ip': 'x.x.x.x',
            'state': 'running',
        }

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'profileId': self._profile_id
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)

        volume = r.json

        volume_id = str(r.json['_id'])


        body = {
            'path': '/data'
        }


        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatusOk(r)

        # Patch back a fake volume ID (normally this would be called from ansible)
        r = self.request('/volumes/%s' % (volume_id), method='PATCH',
                         type='application/json',
                         body=json.dumps({'ec2': {'id': 'vol-00000'}}),
                         user=self._user)

        # Complete the attach operation (normally this would be called from ansible)
        r = self.request('/volumes/%s/clusters/%s/attach/complete' % (volume_id, self._cluster_id),
                         method='PUT', type='application/json',
                         user=self._user, body=json.dumps({"path": "/data"}))

        r = self.request('/volumes/%s' % volume_id, method='DELETE',
                         user=self._user)
        self.assertStatus(r, 400)
        self.assertEquals(delete_volume.call_count, 0)

        # Mock out CloudProvider.get_volume to return an 'in-use' volume
        # That is what calls to attach & attach/complete should have
        # created.
        CloudProvider.return_value.get_volume.return_value = {
            'volume_id': 'vol-00000',
            'state': 'in-use'
        }

        url = '/volumes/%s/detach' % (volume_id)
        r = self.request(url, method='PUT', user=self._user)

        url = '/volumes/%s/detach/complete' % (volume_id)
        r = self.request(url, method='PUT', user=self._user)

        # Mock out CloudProvider.get_volume to return an 'available' volume
        # That is what calls to detach & detach/complete should have
        # created.
        CloudProvider.return_value.get_volume.return_value = {
            'volume_id': 'vol-00000',
            'state': 'available'
        }

        self.assertStatusOk(r)

        r = self.request('/volumes/%s' % volume_id, method='DELETE',
                         user=self._user)
        self.assertStatus(r, 200)
        self.assertEquals(delete_volume.call_count, 1)

    @mock.patch('cumulus_plugin.volume.CloudProvider')
    @mock.patch('cumulus.ansible.tasks.volume.attach_volume.delay')
    def test_attach_volume(self, attach_volume, CloudProvider):

        CloudProvider.return_value.get_volume.return_value = None

        CloudProvider.return_value.get_master_instance.return_value = {
            'instance_id': 'i-00000',
            'private_ip': 'x.x.x.x',
            'public_ip': 'x.x.x.x',
            'state': 'running',
        }

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'fs': 'ext4',
            'profileId': self._profile_id
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)

        volume_id = str(r.json['_id'])

        body = {
            'path': '/data'
        }

        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._user)


        self.assertStatusOk(r)

        expected ={'status': 'attaching',
                   'fs': u'ext4',
                   'name': u'test',
                   'zone': u'us-west-2a',
                   'ec2': {u'id': None},
                   'profileId': self._profile_id,
                   '_id': volume_id,
                   'type': u'ebs',
                   'size': 20}

        self.assertEqual(r.json, expected)


        # Patch back a fake volume ID (normally this would be called from ansible)
        r = self.request('/volumes/%s' % (volume_id), method='PATCH',
                         type='application/json',
                         body=json.dumps({'ec2': {'id': 'vol-00000'}}),
                         user=self._user)

        # Complete the attach operation (normally this would be called from ansible)
        r = self.request('/volumes/%s/clusters/%s/attach/complete' % (volume_id, self._cluster_id),
                         method='PUT', type='application/json',
                         user=self._user, body=json.dumps({"path": "/data"}))



        # Test that the volume has been set up correctly
        r = self.request('/volumes/%s' % volume_id, method='GET',
                         type='application/json', user=self._user)
        expected = {u'status': u'in-use',
                    u'fs': u'ext4',
                    u'name': u'test',
                    u'zone': u'us-west-2a',
                    u'clusterId': self._cluster_id,
                    u'ec2': {u'id': u'vol-00000'},
                    u'profileId': self._profile_id,
                    u'path': u'/data',
                    u'_id': volume_id,
                    u'type': u'ebs',
                    u'size': 20}

        self.assertEquals(r.json, expected)

        # Test that the volume shows up on the cluster under 'volumes' attribute
        r = self.request('/clusters/%s' % self._cluster_id, method='GET',
                         type='application/json', user=self._user)

        self.assertStatusOk(r)

        expected = {
            u'profileId': str(self._profile_id),
            u'status': u'created',
            u'name': u'testing',
            u'userId': str(self._user['_id']),
            u'volumes': [volume_id],
            u'type': u'ec2',
            u'_id': self._cluster_id,
            u'config': {
                u'scheduler': {
                    u'type': u'sge'
                },
                u'ssh': {
                    u'user': u'ubuntu',
                    u'key': str(self._profile_id)
                },
                u'launch': {
                    u'spec': u'default',
                    u'params': {}
                }
            }
        }

        self.assertEqual(r.json, expected)

        # Try to attach volume to a volume that is in use
        CloudProvider.return_value.get_volume.return_value = {
            'id': 'vol-00000', 'state': 'in-use'
        }

        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._user)

        self.assertStatus(r, 400)


        # Try to attach volume to a traditional cluster
        # Patch back to 'attaching' so we don't error out on volume in use
        r = self.request('/volumes/%s' % (volume_id), method='PATCH',
                         type='application/json',
                         body=json.dumps({'status': 'attaching'}),
                         user=self._user)

        url = '/volumes/%s/clusters/%s/attach' % (
            volume_id, self._trad_cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 400)

    @mock.patch('cumulus_plugin.volume.CloudProvider')
    @mock.patch('cumulus.ansible.tasks.volume.attach_volume.delay')
    @mock.patch('cumulus.ansible.tasks.volume.detach_volume.delay')
    def test_detach_volume(self, detach_volume, attach_volume, CloudProvider):
        CloudProvider.return_value.get_volume.return_value = None

        CloudProvider.return_value.get_master_instance.return_value = {
            'instance_id': 'i-00000',
            'private_ip': 'x.x.x.x',
            'public_ip': 'x.x.x.x',
            'state': 'running',
        }

        body = {
            'name': 'testing me',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'profileId': self._profile_id
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


        # Patch back a fake volume ID (normally this would be called from ansible)
        r = self.request('/volumes/%s' % (volume_id), method='PATCH',
                         type='application/json',
                         body=json.dumps({'ec2': {'id': 'vol-00000'}}),
                         user=self._user)

        # Complete the attach operation (normally this would be called from ansible)
        r = self.request('/volumes/%s/clusters/%s/attach/complete' % (volume_id, self._cluster_id),
                         method='PUT', type='application/json',
                         user=self._user, body=json.dumps({"path": "/data"}))


        # Mock out CloudProvider.get_volume to return an 'in-use' volume
        # That is what calls to attach & attach/complete should have
        # created.
        CloudProvider.return_value.get_volume.return_value = {
            'volume_id': 'vol-00000',
            'state': 'in-use'
        }

        url = '/volumes/%s/detach' % (volume_id)
        r = self.request(url, method='PUT', user=self._user)
        self.assertStatusOk(r)

        url = '/volumes/%s/detach/complete' % (volume_id)
        r = self.request(url, method='PUT', user=self._user)
        self.assertStatusOk(r)


        # Assert that detach was called on ec2 object
        self.assertEqual(len(detach_volume.call_args_list),
                         1, "detach was not called")

        r = self.request('/clusters/%s' % self._cluster_id, method='GET',
                         type='application/json', user=self._cumulus)
        self.assertStatusOk(r)

        expected = {
            u'profileId': str(self._profile_id),
            u'status': u'created',
            u'name': u'testing',
            u'userId': str(self._user['_id']),
            u'volumes': [],
            u'type': u'ec2',
            u'_id': self._cluster_id,
            u'config': {
                u'scheduler': {
                    u'type': u'sge'
                },
                u'ssh': {
                    u'user': u'ubuntu',
                    u'key': str(self._profile_id)
                },
                u'launch': {
                    u'spec': u'default',
                    u'params': {}
                }
            }
        }
        self.assertEqual(r.json, expected)

    @mock.patch('cumulus_plugin.volume.CloudProvider')
    @mock.patch('cumulus.ansible.tasks.volume.attach_volume.delay')
    def test_find_volume(self, attach_volume, CloudProvider):

        # Create some test volumes
        body = {
            'name': 'testing me',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'profileId': self._profile_id
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
            'profileId': self._another_profile_id
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

        CloudProvider.return_value.get_volume.return_value = None

        CloudProvider.return_value.get_master_instance.return_value = {
            'instance_id': 'i-00000',
            'private_ip': 'x.x.x.x',
            'public_ip': 'x.x.x.x',
            'state': 'running',
        }


        # Attach a volume
        url = '/volumes/%s/clusters/%s/attach' % (str(volume_1_id), self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatusOk(r)

        # Patch back a fake volume ID (normally this would be called from ansible)
        r = self.request('/volumes/%s' % (volume_1_id), method='PATCH',
                         type='application/json',
                         body=json.dumps({'ec2': {'id': 'vol-00000'}}),
                         user=self._user)

        # Complete the attach operation (normally this would be called from ansible)
        r = self.request('/volumes/%s/clusters/%s/attach/complete' % (volume_1_id, self._cluster_id),
                         method='PUT', type='application/json',
                         user=self._user, body=json.dumps({"path": "/data"}))


        # Search again
        r = self.request('/volumes', method='GET', user=self._user,
                         params=params)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 1, 'Wrong number of volumes returned')

    @mock.patch('cumulus_plugin.volume.CloudProvider')
    @mock.patch('cumulus.ansible.tasks.volume.attach_volume.delay')
    @mock.patch('cumulus.ansible.tasks.volume.detach_volume.delay')
    def test_get_status(self, detach_volume, attach_volume, CloudProvider):
        # Create some test volumes

        CloudProvider.return_value.get_volume.return_value = None
        CloudProvider.return_value.get_master_instance.return_value = {
            'instance_id': 'i-00000',
            'private_ip': 'x.x.x.x',
            'public_ip': 'x.x.x.x',
            'state': 'running',
        }


        body = {
            'name': 'testing me',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'profileId': self._profile_id
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        volume_id = str(r.json['_id'])

        # Should initially be in status 'created'
        url = '/volumes/%s/status' % volume_id
        r = self.request(url, method='GET', user=self._user)
        self.assertStatusOk(r)
        expected = {
            u'status': u'created'
        }
        self.assertEqual(r.json, expected, 'Unexpected status: {}'.format(r.json))


        # Attach the to a fake cluster
        body = {
            'path': '/data'
        }

        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatusOk(r)

        ### Patch back a fake volume ID (normally this would be called from ansible)
        r = self.request('/volumes/%s' % (volume_id), method='PATCH',
                         type='application/json',
                         body=json.dumps({'ec2': {'id': 'vol-00000'}}),
                         user=self._user)

        ### Complete the attach operation (normally this would be called from ansible)
        r = self.request('/volumes/%s/clusters/%s/attach/complete' % (volume_id, self._cluster_id),
                         method='PUT', type='application/json',
                         user=self._user, body=json.dumps({"path": "/data"}))


        url = '/volumes/%s/status' % volume_id
        r = self.request(url, method='GET', user=self._user)
        self.assertStatusOk(r)
        expected = {
            u'status': u'in-use'
        }
        self.assertEqual(r.json, expected, 'Unexpected status'.format(r.json))

        # Detach the volume

        # Mock out CloudProvider.get_volume to return an 'in-use' volume
        # That is what calls to attach & attach/complete should have
        # created.
        CloudProvider.return_value.get_volume.return_value = {
            'volume_id': 'vol-00000',
            'state': 'in-use'
        }

        url = '/volumes/%s/detach' % (volume_id)
        r = self.request(url, method='PUT', user=self._user)
        self.assertStatusOk(r)

        url = '/volumes/%s/detach/complete' % (volume_id)
        r = self.request(url, method='PUT', user=self._user)
        self.assertStatusOk(r)

        url = '/volumes/%s/status' % volume_id
        r = self.request(url, method='GET', user=self._user)
        self.assertStatusOk(r)
        expected = {
            u'status': u'available'
        }
        self.assertEqual(r.json, expected, 'Unexpected status'.format(r.json))


    def test_log(self):
        volume_id = 'vol-1'
        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'profileId': self._profile_id
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        volume_id = str(r.json['_id'])

        # Check that empty log exists for newly created volume
        r = self.request('/volumes/%s/log' % str(volume_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json['log']), 0)

        log_entry = {
            'msg': 'Some message'
        }

        r = self.request('/volumes/546a1844ff34c70456111185/log', method='GET',
                         user=self._user)
        self.assertStatus(r, 404)

        r = self.request('/volumes/%s/log' % str(volume_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        r = self.request('/volumes/%s/log' % str(volume_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        expected_log = {u'log': [{u'msg': u'Some message'}]}
        self.assertEqual(r.json, expected_log)

        r = self.request('/volumes/%s/log' % str(volume_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        r = self.request('/volumes/%s/log' % str(volume_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json['log']), 2)

        r = self.request('/volumes/%s/log' % str(volume_id), method='GET',
                         params={'offset': 1}, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json['log']), 1)

    def test_volume_sse(self):
        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'profileId': self._profile_id
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        volume_id = str(r.json['_id'])

        # connect to volume notification stream
        stream_r = self.request('/notification/stream', method='GET', user=self._user,
                         isJson=False, params={'timeout': 0})
        self.assertStatusOk(stream_r)

        # add a log entry
        log_entry = {
            'msg': 'Some message'
        }
        r = self.request('/volumes/%s/log' % str(volume_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        notifications = self.getSseMessages(stream_r)

        # we get 4 notifications in stream,
        # 1 from cluster 'creating' 1 from cluster 'created' in setUp()
        # 1 from the volume creation and 1 from the volume log
        self.assertEqual(len(notifications), 4, 'Expecting four notifications, received %d' % len(notifications))
        self.assertEqual(notifications[2]['type'], 'volume.status', 'Expecting a message with type \'volume.status\' got: %s' % notifications[2]['type'] )
        self.assertEqual(notifications[3]['type'], 'volume.log', 'Expecting a message with type \'volume.log\' got: %s' % notifications[3]['type'])
