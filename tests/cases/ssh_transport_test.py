import unittest
import StringIO

import mock
import paramiko

from cumulus.transport import get_connection

class MockSSHClient(object):
    def __init__(self, host, username, private_key, private_key_pass, timeout):
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.load_system_host_keys()

    def connect(self):
        self._client.connect('localhost')
        self.transport = self._client.get_transport()

    def execute(self, command, ignore_exit_status=False, source_profile=False):
        return self._client.exec_command(command)

    def close(self):
        if self._client:
            self._client.close()




class SftpClusterConnectionTestCase(unittest.TestCase):

    def __init__(self, name):
        super(SftpClusterConnectionTestCase, self).__init__(name)
        self._test_case_dir = '/tmp/cumulus'
        self._test_dir = '%s/cumulus' % self._test_case_dir
        self._cluster = {
            '_id': 'dummy',
            'type': 'trad',
            'config': {
                'host': 'localhost',
                'ssh': {
                    'user': None,
                    'passphrase': None
                }
            }
        }
        self._girder_token = 'dummy'

    @mock.patch('cumulus.transport.ssh.SSHClient', new=MockSSHClient)
    def setUp(self):
        # Create directory for test case
        with get_connection(self._girder_token, self._cluster) as conn:
            conn.mkdir(self._test_case_dir)
            conn.put(StringIO.StringIO(), '%s/test.txt' % self._test_case_dir)


    @mock.patch('cumulus.transport.ssh.SSHClient', new=MockSSHClient)
    def tearDown(self):
        try:
            with get_connection(self._girder_token, self._cluster) as conn:
                conn.execute('rm -rf %s' % self._test_case_dir)
        except Exception:
            raise

    @mock.patch('cumulus.transport.ssh.SSHClient', new=MockSSHClient)
    def test_list(self):
        with get_connection(self._girder_token, self._cluster) as conn:
            for path in conn.list(self._test_case_dir):
                self.assertEqual(len(path.keys()), 6)
                self.assertTrue('name' in path)
                self.assertTrue('group' in path)
                self.assertTrue('user' in path)
                self.assertTrue('mode' in path)
                self.assertTrue('date' in path)
                self.assertTrue('size' in path)

if __name__ == '__main__':
    unittest.main()
