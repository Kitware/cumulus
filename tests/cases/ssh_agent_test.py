#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2016 Kitware Inc.
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
import paramiko
import tempfile
import os
import threading
from cumulus.ssh.agent import Agent

class SshAgentTestCase(unittest.TestCase):

    def setUp(self):
        self._passphrase = 'open up please!'
        self._private_key = paramiko.rsakey.RSAKey.generate(bits=2048)
        (_, self._private_key_filepath) = tempfile.mkstemp()
        self._private_key.write_private_key_file(self._private_key_filepath,
                                       password=self._passphrase)
        self._public_key = self._private_key.get_base64()


    def tearDown(self):
        if os.path.exists(self._private_key_filepath):
            os.remove(self._private_key_filepath)

    def test_agent(self):
        _agent_client = None
        _agent = None
        test_data = 'sign me up!'
        _socket_path = None
        try:
            _agent = Agent(self._private_key_filepath, self._passphrase)
            (_socket_path, _) = _agent.listen()
            os.environ['SSH_AUTH_SOCK'] = _socket_path

            def _accept():
                _agent.accept()

            # Run accept in another thread
            t = threading.Thread(target=_accept)
            t.start()

            # Create client agent and request a list of keys, we should get one
            _agent_client = paramiko.Agent()
            keys = _agent_client.get_keys()
            self.assertEqual(len(keys), 1)

            key = keys[0]
            self.assertEqual(key.get_base64(), self._public_key)

            # Now test signed some data
            sign = key.sign_ssh_data(test_data)
            self.assertTrue(self._private_key.verify_ssh_sig(
                test_data, paramiko.Message(sign)))
        finally:
            if _agent:
                _agent.close()
            if _agent_client:
                _agent_client.close()

        if _socket_path:
            self.assertFalse(os.path.exists(_socket_path))


