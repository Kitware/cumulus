import argparse
import unittest
import json
import time
import traceback
import paramiko
from jsonpath_rw import parse

from base_integration_test import BaseIntegrationTest, base_parser
from girder_client import HttpError

class TradIntegrationTest(BaseIntegrationTest):

    def __init__(self, name):
        super(TradIntegrationTest, self).__init__(name,
                TradIntegrationTest.GIRDER_URL, TradIntegrationTest.GIRDER_USER,
                TradIntegrationTest.GIRDER_PASSWORD)

    def tearDown(self):
        super(TradIntegrationTest, self).tearDown()
        try:
            url = 'clusters/%s' % self._cluster_id
            self._client.delete(url)
        except Exception:
            traceback.print_exc()

    def create_cluster(self):
        body = {
            'config': {
                'ssh': {
                    'user': TradIntegrationTest.USER
                },
                'host': TradIntegrationTest.HOST
            },
            'name': 'TradIntegrationTest',
            'type': 'trad'
        }

        r = self._client.post('clusters', data=json.dumps(body))
        self._cluster_id = r['_id']

        sleeps = 0
        while True:
            time.sleep(1)
            r = self._client.get('clusters/%s/status' % self._cluster_id)

            if r['status'] == 'created':
                break

            if sleeps > 9:
                self.fail('Cluster never moved into created state')

            sleeps += 1

        r = self._client.get('clusters/%s' % self._cluster_id)

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.load_system_host_keys()
        client.connect(TradIntegrationTest.HOST, username=TradIntegrationTest.USER)
        key = parse('config.ssh.publicKey').find(r)

        if not key:
            self.fail('No public key generated')

        key = key[0].value

        stdin, stdout, stderr = client.exec_command('echo "%s" >> ~/.ssh/authorized_keys' % key)
        self.assertFalse(stdout.read())
        self.assertFalse(stderr.read())

        # Now test the connection
        r = self._client.put('clusters/%s/start' % self._cluster_id)
        sleeps = 0
        while True:
            time.sleep(1)
            r = self._client.get('clusters/%s/status' % self._cluster_id)

            if r['status'] == 'running':
                break

            if sleeps > 9:
                self.fail('Cluster never moved into created state')
            sleeps += 1

    def test(self):
        try:
            self.create_cluster()
            self.create_script()
            self.create_input()
            self.create_output_folder()
            self.create_job()
            self.submit_job()
            self.assert_output()
        except HttpError as error:
            self.fail(error.responseText)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(parents=[base_parser])
    parser.add_argument('-n', '--host', help='', required=True)
    parser.add_argument('-u', '--user', help='', required=True)

    args = parser.parse_args()

    TradIntegrationTest.USER = args.user
    TradIntegrationTest.HOST = args.host

    TradIntegrationTest.GIRDER_USER = args.girder_user
    TradIntegrationTest.GIRDER_PASSWORD = args.girder_password
    TradIntegrationTest.GIRDER_URL = args.girder_url

    suite = unittest.TestLoader().loadTestsFromTestCase(TradIntegrationTest)
    unittest.TextTestRunner().run(suite)
