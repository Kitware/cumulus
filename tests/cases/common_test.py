import unittest
import mock
import httmock
import os
import json
from jsonpath_rw import parse

import cumulus
from cumulus.ssh.tasks import key
from cumulus.starcluster.tasks.common import get_ssh_connection

class CommonTestCase(unittest.TestCase):
    def setUp(self):
        self._get_config_called = False

        self._cluster_id = '55c3a698f6571011a48f6817'
        self._key_path = os.path.join(cumulus.config.ssh.keyStore, self._cluster_id)
        with open(self._key_path, 'w') as fp:
            fp.write('bogus')

    def tearDown(self):
        try:
            os.remove(self._key_path)
        except OSError:
            pass

    @mock.patch('starcluster.sshutils.SSHClient.connect')
    def test_get_ssh_connection_trad(self, connect):
        cluster = {
            '_id': self._cluster_id,
            'config': {
                'ssh': {
                    'username': 'bob',
                    'passphrase': 'test'
                },
                'hostname': 'localhost'
            },
            'type': 'trad'
        }

        conn = get_ssh_connection('girder_token', cluster)

    @mock.patch('starcluster.config.StarClusterConfig')
    def test_get_ssh_connection_ec2(self, config):
        cluster = {
            'config': {
                '_id': 'dummy'
            },
            'type': 'ec2',
            'name': 'mycluster'
        }

        dummy_config = 'my dummy config'

        def _get_config(url, request):
            headers = {
                'content-length': len(dummy_config),
                'content-type': 'text/plain'
            }

            self._get_config_called  = True
            return httmock.response(200, dummy_config, headers, request=request)


        config_url = '/api/v1/starcluster-configs/dummy'
        config = httmock.urlmatch(
            path=r'^%s$' % config_url, method='GET')(_get_config)

        with httmock.HTTMock(config):
            get_ssh_connection('girder_token', cluster)

        self.assertTrue(self._get_config_called, 'The cluster configuration was not fetched')



