import unittest
import mock
import httmock
import json
import re

from cumulus.trad.tasks import cluster

class TradClusterTestCase(unittest.TestCase):

    def setUp(self):
        self._expected_status = 'error'
        self._set_status_called  = False
        self._set_status_valid  = False

    def normalize(self, data):
        str_data = json.dumps(data, default=str)
        str_data = re.sub(r'[\w]{64}', 'token', str_data)

        return json.loads(str_data)

    def assertCalls(self, actual, expected, msg=None):
        calls = []
        for (args, kwargs) in self.normalize(actual):
            calls.append((args, kwargs))

        self.assertListEqual(self.normalize(calls), expected, msg)


    @mock.patch('cumulus.starcluster.logging.StarClusterLogHandler')
    @mock.patch('cumulus.trad.tasks.cluster.get_ssh_connection')
    def test_connection(self, get_ssh_connection, StarClusterLogHandler):

        def valid(self):
            return True

        cluster_id = 'dummy_id'
        cluster_model = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'ssh': {
                    'user': 'bob'
                },
                'host': 'myhost'
            },
            '_id': cluster_id
        }

        self._set_call_value_index = 0
        def _set_status(url, request):
            expected = {'status': self._expected_status}
            self._set_status_called = True
            self._set_status_valid = json.loads(request.body) == expected
            self._set_status_request = request.body

            return httmock.response(200, None, {}, request=request)

        status_update_url = '/api/v1/clusters/%s' % cluster_id
        set_status = httmock.urlmatch(
            path=r'^%s$' % status_update_url, method='PATCH')(_set_status)

        with httmock.HTTMock(set_status):
            cluster.test_connection(cluster_model, **{'girder_token': 's', 'log_write_url': 'http://localhost/log'})

        self.assertTrue(self._set_status_called, 'Set status endpoint not called')
        self.assertTrue(self._set_status_valid,
                        'Set status endpoint called in incorrect content: %s'
                            % self._set_status_request)

        # Mock our ssh calls and try again
        def _get_cluster(url, request):
            content =   {
                "_id": "55ef53bff657104278e8b185",
                "config": {
                    "host": "ulmus",
                    "ssh": {
                        "publicKey": "ssh-rsa dummy",
                        'passphrase': 'dummy',
                        "user": "test"
                    }
                }
            }

            content = json.dumps(content)
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }
            return httmock.response(200, content, headers, request=request)

        cluster_url = '/api/v1/clusters/%s' % cluster_id
        get_cluster = httmock.urlmatch(
            path=r'^%s$' % cluster_url, method='GET')(_get_cluster)


        ssh = get_ssh_connection.return_value.__enter__.return_value
        ssh.execute.return_value = ['/usr/bin/qsub']
        self._expected_status = 'running'
        with httmock.HTTMock(set_status, get_cluster):
            cluster.test_connection(cluster_model, **{'girder_token': 's', 'log_write_url': 'http://localhost/log'})

        self.assertTrue(self._set_status_called, 'Set status endpoint not called')
        self.assertTrue(self._set_status_valid,
                        'Set status endpoint called in incorrect content: %s'
                            % self._set_status_request)


