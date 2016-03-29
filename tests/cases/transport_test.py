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

import unittest
import mock
import httmock
import os
import json
from jsonpath_rw import parse

import cumulus
from cumulus.ssh.tasks import key
from cumulus.transport import get_connection
from cumulus.transport.ssh import SshClusterConnection

class TransportTestCase(unittest.TestCase):
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

    @mock.patch('cumulus.transport.ssh.paramiko.RSAKey.from_private_key_file')
    @mock.patch('cumulus.transport.ssh.paramiko.SSHClient.connect')
    def test_get_ssh_connection(self, connect, from_private_key_file):
        cluster = {
            '_id': self._cluster_id,
            'config': {
                'ssh': {
                    'user': 'bob',
                    'key': self._cluster_id,
                    'passphrase': 'test'
                },
                'host': 'localhost'
            },
            'type': 'trad'
        }

        with get_connection('girder_token', cluster) as ssh:
            self.assertTrue(isinstance(ssh, SshClusterConnection))


