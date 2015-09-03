import unittest
import mock
import httmock
import json
import re
import os
import starcluster

from cumulus.starcluster.tasks import cluster
from starcluster.config import StarClusterConfig
from __builtin__ import True

class MockEasyEC2():
    def get_max_instances(self):
        return 10

    def get_running_instance_count(self):
        return 5

class MockStarClusterConfig(StarClusterConfig):
    def __init__(self, *args, **kw):
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'fixtures', 'config'))
        super(MockStarClusterConfig, self).__init__(config_file=path)

    def get_easy_ec2(self):
        return MockEasyEC2()

class ClusterTestCase(unittest.TestCase):

    def setUp(self):
        self._get_status_called  = False
        self._set_status_called  = False

    def normalize(self, data):
        str_data = json.dumps(data, default=str)
        str_data = re.sub(r'[\w]{64}', 'token', str_data)

        return json.loads(str_data)

    def assertCalls(self, actual, expected, msg=None):
        calls = []
        for (args, kwargs) in self.normalize(actual):
            calls.append((args, kwargs))

        self.assertListEqual(self.normalize(calls), expected, msg)

    @mock.patch('starcluster.config.StarClusterConfig', new=MockStarClusterConfig)
    @mock.patch('starcluster.config.awsutils.EasyEC2', new=MockEasyEC2)
    @mock.patch('cumulus.starcluster.logging.StarClusterLogHandler')
    def test_start_cluster_max_instance_limit(self, logging):

        def valid(self):
            return True

        starcluster.cluster.ClusterValidator.validate_required_settings = valid

        cluster_id = 'dummy_id'
        cluster_model = {
            '_id': cluster_id,
            'config': {
                '_id': 'dummy_config_id'
            },
            'name': 'dummy_cluster_name',
            'template': 'dummy_template'
        }

        def _get_status(url, request):
            content = {
                'status': 'initializing'
            }
            content = json.dumps(content)
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            self._get_status_called  = True
            return httmock.response(200, content, headers, request=request)

        self._set_call_value_index = 0
        def _set_status(url, request):
            expected = [{'status': 'initializing'}, {'status': 'error'}]
            self._set_status_called = json.loads(request.body) == expected[self._set_call_value_index]
            self.assertEqual(json.loads(request.body),
                             expected[self._set_call_value_index],
                              'Unexpected status update body')
            self._set_call_value_index += 1
            return httmock.response(200, None, {}, request=request)

        status_url = '/api/v1/clusters/%s/status' % cluster_id
        get_status = httmock.urlmatch(
            path=r'^%s$' % status_url, method='GET')(_get_status)

        status_update_url = '/api/v1/clusters/%s' % cluster_id
        set_status = httmock.urlmatch(
            path=r'^%s$' % status_update_url, method='PATCH')(_set_status)

        with httmock.HTTMock(get_status, set_status):
            cluster.start_cluster(cluster_model, **{'girder_token': 's', 'log_write_url': 1})



