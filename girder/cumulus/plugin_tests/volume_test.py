from tests import base
import json
import mock
import re
import httmock
import cherrypy
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

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def setUp(self, *args):
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

    #@mock.patch('urllib2.urlopen')
    @mock.patch('starcluster.config.StarClusterConfig')
    def test_create(self, MockStarClusterConfig):
        #mock_urlopen.return_value = mock.Mock()
        #mock_urlopen.return_value.readline.side_effect = self._ini_file
        volume_id = 'vol-1'
        instance = MockStarClusterConfig.return_value
        instance \
            .get_easy_ec2.return_value \
            .create_volume.return_value.id = volume_id

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'config': {
                '_id': self._config_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        expected = {
            u'name': u'test',
            u'zone': u'us-west-2a',
            u'type': u'ebs',
            u'size': 20,
            u'ec2': {
                u'id': volume_id
            },
            u'config': {
                u'_id': self._config_id
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
            'config': {
                '_id': self._config_id
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
            u'config': {
                u'_id': self._config_id
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
            'config': {
                '_id': self._config_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 400)

        # Create a volume without a zone
        test_zone = 'test-zone'
        instance \
            .get_easy_ec2.return_value \
            .get_zones.return_value = [EasyDict({'name': test_zone})]

        body = {
            'name': 'zoneless',
            'size': 20,
            'type': 'ebs',
            'config': {
                '_id': self._config_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        self.assertEqual(r.json['zone'], test_zone, 'Volume created in wrong zone')


    @mock.patch('starcluster.config.StarClusterConfig')
    def test_get(self, MockStarClusterConfig):
        volume_id = 'vol-1'
        instance = MockStarClusterConfig.return_value
        instance \
            .get_easy_ec2.return_value \
            .create_volume.return_value.id = volume_id

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'config': {
                '_id': self._config_id
            }
        }

        expected = {
            u'name': u'test',
            u'zone': u'us-west-2a',
            u'ec2': {
                u'id': u'vol-1'
            },
            u'type':
            u'ebs',
            u'size': 20,
            u'config': {
                u'_id': self._config_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        volume = r.json
        volume_id = str(r.json['_id'])

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

    @mock.patch('starcluster.config.StarClusterConfig')
    def test_delete(self, MockStarClusterConfig):
        volume_id = 'vol-1'
        instance = MockStarClusterConfig.return_value
        instance \
            .get_easy_ec2.return_value \
            .create_volume.return_value.id = volume_id
        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'config': {
                '_id': self._config_id
            }
        }

        r = self.request('/volumes', method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 201)
        volume = r.json
        volume_id = str(r.json['_id'])

        # Try and delete any attached volume
        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'available'

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
        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'in-use'
        url = '/volumes/%s/detach' % (volume_id)
        r = self.request(url, method='PUT', user=self._cumulus)
        self.assertStatusOk(r)

        r = self.request('/volumes/%s' % volume_id, method='DELETE',
                         user=self._cumulus)
        self.assertStatus(r, 200)

    @mock.patch('starcluster.config.StarClusterConfig')
    def test_attach_volume(self, MockStarClusterConfig):
        ec2_volume_id = 'vol-1'
        instance = MockStarClusterConfig.return_value
        instance \
            .get_easy_ec2.return_value \
            .create_volume.return_value.id = ec2_volume_id

        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'available'

        body = {
            'name': 'test',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'config': {
                '_id': self._config_id
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

        r = self.request('/starcluster-configs/%s' % self._config_id, method='GET',
                         type='application/json', user=self._cumulus)
        self.assertStatusOk(r)
        expected = {u'plugin': [{u'requests-installer': {u'setup_class': u'starcluster.plugins.pypkginstaller.PyPkgInstaller', u'packages': u'requests, requests-toolbelt'}}], u'vol': [{u'test': {u'mount_path': u'/data', u'volume_id': u'vol-1'}}], u'global': {u'default_template': u''}, u'aws': [{u'info': {u'aws_user_id': u'cjh', u'aws_region_name': u'us-west-2', u'aws_region_host': u'ec2.us-west-2.amazonaws.com', u'aws_access_key_id': u'AKRWOVFSYTVQ2Q', u'aws_secret_access_key': u'3z/PSglaGt1MGtGJ'}}], u'cluster': [{u'default_cluster': {u'plugins': u'requests-installer', u'volumes': u'test', u'availability_zone': u'us-west-2a', u'master_instance_type': u't1.micro', u'cluster_user': u'ubuntu',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              u'public_ips': u'True', u'keyname': u'cjh', u'cluster_size': u'2', u'node_image_id': u'ami-b2badb82', u'node_instance_type': u't1.micro', u'permissions': u'ssh, http, paraview, http8080'}}], u'key': [{u'cjh': {u'key_location': u'/home/cjh/work/source/cumulus/cjh.pem'}}], u'permission': [{u'http': {u'to_port': u'80', u'from_port': u'80', u'ip_protocol': u'tcp'}}, {u'http8080': {u'to_port': u'8080', u'from_port': u'8080', u'ip_protocol': u'tcp'}}, {u'https': {u'to_port': u'443', u'from_port': u'443', u'ip_protocol': u'tcp'}}, {u'paraview': {u'to_port': u'11111', u'from_port': u'11111', u'ip_protocol': u'tcp'}}, {u'ssh': {u'to_port': u'22', u'from_port': u'22', u'ip_protocol': u'tcp'}}]}
        self.assertEqual(
            expected, r.json, 'Config was not successfully updated')

        # Try to attach volume that is already attached
        instance.reset_mock()
        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'in-use'
        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 400)

        # Try to attach volume that is currently being created
        instance.reset_mock()
        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'creating'
        url = '/volumes/%s/clusters/%s/attach' % (volume_id, self._cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)

        self.assertStatus(r, 400)
        # Try to attach volume to traditional cluster
        instance.reset_mock()
        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'creating'
        url = '/volumes/%s/clusters/%s/attach' % (
            volume_id, self._trad_cluster_id)
        r = self.request(url, method='PUT',
                         type='application/json', body=json.dumps(body),
                         user=self._cumulus)
        self.assertStatus(r, 400)

    @mock.patch('starcluster.config.StarClusterConfig')
    def test_detach_volume(self, MockStarClusterConfig):
        ec2_volume_id = 'vol-1'
        instance = MockStarClusterConfig.return_value
        instance \
            .get_easy_ec2.return_value \
            .create_volume.return_value.id = ec2_volume_id

        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'available'

        body = {
            'name': 'testing me',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'config': {
                '_id': self._config_id
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
        instance.reset_mock()
        instance \
            .get_easy_ec2.return_value \
            .create_volume.return_value.id = ec2_volume_id

        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'in-use'

        url = '/volumes/%s/detach' % (volume_id)
        r = self.request(url, method='PUT', user=self._user)
        self.assertStatusOk(r)

        # Assert that detach was called on ec2 object
        self.assertEqual(len(instance.get_easy_ec2.return_value.get_volume
                             .return_value.detach.call_args_list),
                         1, "detach was not called")

        r = self.request('/starcluster-configs/%s' % self._config_id, method='GET',
                         type='application/json', user=self._cumulus)
        expected = {u'plugin': [{u'requests-installer': {u'setup_class': u'starcluster.plugins.pypkginstaller.PyPkgInstaller', u'packages': u'requests, requests-toolbelt'}}], u'vol': [], u'global': {u'default_template': u''}, u'aws': [{u'info': {u'aws_user_id': u'cjh', u'aws_region_name': u'us-west-2', u'aws_region_host': u'ec2.us-west-2.amazonaws.com', u'aws_access_key_id': u'AKRWOVFSYTVQ2Q', u'aws_secret_access_key': u'3z/PSglaGt1MGtGJ'}}], u'cluster': [{u'default_cluster': {u'plugins': u'requests-installer', u'availability_zone': u'us-west-2a', u'master_instance_type': u't1.micro', u'cluster_user': u'ubuntu', u'public_ips': u'True', u'keyname': u'cjh',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  u'cluster_size': u'2', u'node_image_id': u'ami-b2badb82', u'node_instance_type': u't1.micro', u'permissions': u'ssh, http, paraview, http8080'}}], u'key': [{u'cjh': {u'key_location': u'/home/cjh/work/source/cumulus/cjh.pem'}}], u'permission': [{u'http': {u'to_port': u'80', u'from_port': u'80', u'ip_protocol': u'tcp'}}, {u'http8080': {u'to_port': u'8080', u'from_port': u'8080', u'ip_protocol': u'tcp'}}, {u'https': {u'to_port': u'443', u'from_port': u'443', u'ip_protocol': u'tcp'}}, {u'paraview': {u'to_port': u'11111', u'from_port': u'11111', u'ip_protocol': u'tcp'}}, {u'ssh': {u'to_port': u'22', u'from_port': u'22', u'ip_protocol': u'tcp'}}]}
        self.assertStatusOk(r)
        self.assertEqual(expected, r.json, 'Config was not updated correctly')

    @mock.patch('starcluster.config.StarClusterConfig')
    def test_find_volume(self, MockStarClusterConfig):
        ec2_volume_id = 'vol-1'
        instance = MockStarClusterConfig.return_value
        instance \
            .get_easy_ec2.return_value \
            .create_volume.return_value.id = ec2_volume_id

        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'available'

        # Create some test volumes
        body = {
            'name': 'testing me',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'config': {
                '_id': self._config_id
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
            'config': {
                '_id': self._config_id
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

    @mock.patch('starcluster.config.StarClusterConfig')
    def test_get_status(self, MockStarClusterConfig):
        ec2_volume_id = 'vol-1'
        instance = MockStarClusterConfig.return_value
        instance \
            .get_easy_ec2.return_value \
            .create_volume.return_value.id = ec2_volume_id

        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'available'

        # Create some test volumes
        body = {
            'name': 'testing me',
            'size': 20,
            'zone': 'us-west-2a',
            'type': 'ebs',
            'config': {
                '_id': self._config_id
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

        instance \
            .get_easy_ec2.return_value \
            .get_volume.return_value \
            .update.return_value = 'in-use'
        r = self.request(url, method='GET', user=self._user)
        self.assertStatusOk(r)
        expected = {
            u'status': u'in-use'
        }
        self.assertEqual(r.json, expected, 'Unexpected status')