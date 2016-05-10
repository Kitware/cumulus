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

    class ConnectionException(Exception):
        pass

    class AgentConnection(object):
        def __init__(self, agent, conn):
            self._agent = agent
            self._conn = conn

        def _read_all(self, size):
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
            msg = asbytes(msg)
            self._conn.send(struct.pack('>I', len(msg)) + msg)

        def _read_message(self):
            l = self._read_all(4)
            msg = Message(self._read_all(struct.unpack('>I', l)[0]))

            return ord(msg.get_byte()), msg

        def _handle_identities_request(self, msg):
            msg = Message()
            msg.add_byte(byte_chr(SSH2_AGENT_IDENTITIES_ANSWER))
            msg.add_int(1)
            msg.add_string(self._agent.key.asbytes())
            msg.add_string('')
            self._send_message(msg)

        def _handle_sign_request(self, msg):
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
            try:
                while True:
                    msg_type, msg = self._read_message()

                    if msg_type == SSH2_AGENT_REQUEST_IDENTITIES:
                        self._handle_identities_request(msg)
                    elif msg_type == SSH2_AGENT_REQUEST_SIGN:
                        self._handle_sign_request(msg)
            except Agent.ConnectionException:
                self._conn.close()


    def __init__(self, key_path, passphrase):
        self._key = RSAKey.from_private_key_file(key_path, password=passphrase)

    def accept(self):
        (conn, _) = self._conn.accept()
        agent_conn = Agent.AgentConnection(self, conn)
        agent_conn.run()

    def listen(self):
        self._dir = tempfile.mkdtemp('cumulus')
        os.chmod(self._dir, stat.S_IRWXU)
        self._file = self._dir + '/cumulusproxy.ssh'
        self._conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._conn.bind(self._file)
        self._conn.listen(1)

        return self._file

    @property
    def key(self):
        return self._key

    def close(self):
        self._conn.close()
        shutil.rmtree(self._dir)


