import unittest
import mock
import httmock
import os
import json
from jsonpath_rw import parse

import cumulus
from cumulus.ssh.tasks import key
from cumulus.starcluster.common import get_ssh_connection

class CommonTestCase(unittest.TestCase):
    def setUp(self):
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
                    'user': 'bob',
                    'passphrase': 'test'
                },
                'host': 'localhost'
            },
            'type': 'trad'
        }

        get_ssh_connection('girder_token', cluster)

    @mock.patch('cumulus.starcluster.common.create_config_request')
    @mock.patch('starcluster.config.StarClusterConfig')
    def test_get_ssh_connection_ec2(self, StarClusterConfig,
                                    create_config_request):
        cluster = {
            '_id': self._cluster_id,
            'config': {
                '_id': 'dummy'
            },
            'type': 'ec2',
            'name': 'mycluster'
        }

        get_ssh_connection('girder_token', cluster)
        self.assertEqual(len(create_config_request.call_args_list),
                         1, 'The cluster configuration was not fetched')



