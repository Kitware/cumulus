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
import socket
import tempfile
import os
import stat
import struct
import shutil

from paramiko.agent import SSH2_AGENT_IDENTITIES_ANSWER
from paramiko.agent import SSH2_AGENT_SIGN_RESPONSE
from paramiko.agent import AgentKey
from paramiko import Message
from paramiko.py3compat import byte_chr
from paramiko.common import asbytes
from paramiko import RSAKey

SSH2_AGENT_REQUEST_IDENTITIES = 11
SSH2_AGENT_REQUEST_SIGN = 13
SSH2_AGENT_FAILURE = 5

class Agent(object):
    """
    This class implements the SSH agent protocol to allow the use of encrypted
    key where the passphrase is held in Girder. It is currently used when
    runing ansible playbooks.
    """
    class ConnectionException(Exception):
        pass

    class AgentConnection(object):
        """
        A agent client/server connection
        """
        def __init__(self, agent, conn):
            self._agent = agent
            self._conn = conn

        def _read_all(self, size):
            """
            Read n bytes

            :param size: The number of bytes to read
            :type size: int
            :returns: The bytes read.
            """
            bytes = self._conn.recv(size)
            while len(bytes) < size:
                if len(bytes) == 0:
                    raise Agent.ConnectionException('Connection closed')
                read = self._conn.recv(size - len(bytes))
                if len(read) == 0:
                    raise Agent.ConnectionException('Connection closed')
                bytes += read

            return bytes

        def _send_message(self, msg):
            """
            Send an out going message.

            :param msg: The outgoing message.
            :type msg: paramiko.Message
            """
            msg = asbytes(msg)
            self._conn.send(struct.pack('>I', len(msg)) + msg)

        def _read_message(self):
            """
            Read an incoming message.

            :returns: A typle containing the message type and message data
            """
            l = self._read_all(4)
            msg = Message(self._read_all(struct.unpack('>I', l)[0]))

            return ord(msg.get_byte()), msg

        def _handle_identities_request(self):
            """
            Handle a identities request. We just provide the one key we where
            provided with when created.
            """
            msg = Message()
            msg.add_byte(byte_chr(SSH2_AGENT_IDENTITIES_ANSWER))
            msg.add_int(1)
            msg.add_string(self._agent.key.asbytes())
            msg.add_string('')
            self._send_message(msg)

        def _handle_sign_request(self, msg):
            """
            Handle a sign request
            :param msg: The incoming sign request.
            :type msg: paramiko.Message
            """
            key = AgentKey(None, msg.get_binary())
            data = msg.get_string()
            msg.get_int()

            if key.get_base64() != self._agent.key.get_base64():
                error = Message()
                error.add_byte(byte_chr(SSH2_AGENT_FAILURE))
                self._send_message(error)
            else:
                response = Message()
                response.add_byte(byte_chr(SSH2_AGENT_SIGN_RESPONSE))
                sig = self._agent.key.sign_ssh_data(data)
                response.add_string(sig.asbytes())
                self._send_message(response)

        def run(self):
            """
            Accepts and processes identity and signing requests.
            """
            try:
                while True:
                    msg_type, msg = self._read_message()

                    if msg_type == SSH2_AGENT_REQUEST_IDENTITIES:
                        self._handle_identities_request()
                    elif msg_type == SSH2_AGENT_REQUEST_SIGN:
                        self._handle_sign_request(msg)
            except Agent.ConnectionException:
                self._conn.close()


    def __init__(self, key_path, passphrase):
        """
        Create a new agent.

        :param key_path: The path to the private key this agent is going to use
                         for signing.
        :type key_path: string
        :param passphrase: The passphrase to unlock the key.
        :type passphrase: string
        """
        self._key = RSAKey.from_private_key_file(key_path, password=passphrase)

    def accept(self):
        """
        Accept an incoming client connection and then start the processing loop
        to process incoming requests.
        """
        (conn, _) = self._conn.accept()
        agent_conn = Agent.AgentConnection(self, conn)
        agent_conn.run()

    def listen(self):
        """
        Start the agent listening on a random socket.

        :returns: A tuple containing the path to the socket and socket fd.
        """
        self._dir = tempfile.mkdtemp('cumulus')
        os.chmod(self._dir, stat.S_IRWXU)
        self._file = self._dir + '/cumulusproxy.ssh'
        self._conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._conn.bind(self._file)
        self._conn.listen(1)

        return (self._file, self._conn)

    @property
    def key(self):
        """
        The key we are using for signing.
        """
        return self._key

    def close(self):
        """
        Stop listening and cleanup the socket.
        """
        self._conn.close()
        shutil.rmtree(self._dir)


