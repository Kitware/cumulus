import unittest
import mock
import httmock
import cumulus
import json
import re
from cumulus.starcluster.tasks import job
from celery.app import task

class MockMaster:
    execute_stack = []
    def __init__(self):
        self.ssh = mock.MagicMock()

        self.ssh.execute.side_effect = MockMaster.execute_stack

class MockCluster:
    def __init__(self):
        self.master_node = MockMaster()
        self.cluster_user = 'bob'


class MockClusterManager:
    def __init__(self):
        self.cluster = MockCluster()
    def get_cluster(self, name):
        return self.cluster

class MockStarClusterConfig():
    def __init__(self, *args, **kw):
        self.cluster_manager = MockClusterManager()

    def load(self):
        pass

    def get_cluster_manager(self):
        return self.cluster_manager


class MockContext(task.Context):

    def __init__(self, *args, **kwargs):
        self.id = 'test_uuid'
        self.delivery_info = {'exchange': 'jobs'}

    def retry(self,args=None, kwargs=None, exc=None, throw=True, eta=None, countdown=None, max_retries=None, **options):
        pass

def capture_mock(func):
    pass

def _write_config_file():
    return ''

class JobTestCase(unittest.TestCase):

    def setUp(self):
        self._get_status_called  = False
        self._set_status_called  = False
        self._upload_job_output = cumulus.starcluster.tasks.job.upload_job_output.delay = mock.Mock()

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
    @mock.patch('cumulus.starcluster.logging')
    @mock.patch('cumulus.starcluster.tasks.common._write_config_file')
    def test_monitor_job_terminated(self, _write_config_file, logging):
        _write_config_file.return_value = 'dummy file path'

        job_id = 'dummy'
        cluster = {
            'name': 'dummy',
             'configId': 'dummy'
        }
        job_model = {
            '_id': job_id,
            'sgeId': 'dummy',
            'name': 'dummy',
            'output': []
        }

        MockMaster.execute_stack = ['qstat output']

        def _get_status(url, request):
            content = {
                'status': 'terminating'
            }
            content = json.dumps(content)
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            self._get_status_called  = True
            return httmock.response(200, content, headers, request=request)

        def _set_status(url, request):
            self._set_status_called = True
            expected = {'status': 'terminated', 'output': [], 'timings': {}}
            self.assertEqual(json.loads(request.body), expected, 'Unexpected status update body')
            return httmock.response(200, None, {}, request=request)

        status_url = '/api/v1/jobs/%s/status' % job_id
        get_status = httmock.urlmatch(
            path=r'^%s$' % status_url, method='GET')(_get_status)

        status_update_url = '/api/v1/jobs/%s' % job_id
        set_status = httmock.urlmatch(
            path=r'^%s$' % status_update_url, method='PATCH')(_set_status)

        with httmock.HTTMock(get_status, set_status):
            job.monitor_job(cluster, job_model, **{'girder_token': 's', 'log_write_url': 1})

        self.assertTrue(self._get_status_called, 'Expect get status endpoint to be hit')
        self.assertTrue(self._set_status_called, 'Expect set status endpoint to be hit')

    @mock.patch('starcluster.config.StarClusterConfig', new=MockStarClusterConfig)
    @mock.patch('starcluster.logger')
    @mock.patch('cumulus.starcluster.logging')
    @mock.patch('cumulus.starcluster.tasks.common._write_config_file')
    def test_monitor_job_complete(self, _write_config_file, logging,logger):
        _write_config_file.return_value = 'dummy file path'

        job_id = 'dummy'
        cluster = {
            'name': 'dummy',
             'configId': 'dummy'
        }
        job_model = {
            '_id': job_id,
            'sgeId': 'dummy',
            'name': 'dummy',
            'output': []
        }

        MockMaster.execute_stack = ['qstat output']

        def _get_status(url, request):
            content = {
                'status': 'running'
            }
            content = json.dumps(content)
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            self._get_status_called  = True
            return httmock.response(200, content, headers, request=request)

        def _set_status(url, request):
            expected = {'status': 'uploading', 'output': [], 'timings': {}}
            self._set_status_called = json.loads(request.body) == expected
            self.assertEqual(json.loads(request.body), expected, 'Unexpected status update body')
            return httmock.response(200, None, {}, request=request)

        status_url = '/api/v1/jobs/%s/status' % job_id
        get_status = httmock.urlmatch(
            path=r'^%s$' % status_url, method='GET')(_get_status)

        status_update_url = '/api/v1/jobs/%s' % job_id
        set_status = httmock.urlmatch(
            path=r'^%s$' % status_update_url, method='PATCH')(_set_status)

        with httmock.HTTMock(get_status, set_status):
            job.monitor_job(cluster, job_model, **{'girder_token': 's', 'log_write_url': 1})

        self.assertTrue(self._get_status_called, 'Expect get status endpoint to be hit')
        self.assertTrue(self._set_status_called, 'Expect set status endpoint to be hit')
        expected_calls = [[[{u'configId': u'dummy', u'name': u'dummy'}, {u'status': u'uploading', u'output': [], u'_id': u'dummy', u'sgeId': u'dummy', u'name': u'dummy'}], {u'girder_token': u's', u'log_write_url': 1, u'config_url': None, u'job_dir': u'dummy'}]]
        self.assertCalls(self._upload_job_output.call_args_list, expected_calls)

    @mock.patch('starcluster.config.StarClusterConfig', new=MockStarClusterConfig)
    @mock.patch('starcluster.logger')
    @mock.patch('cumulus.starcluster.logging')
    @mock.patch('cumulus.starcluster.tasks.common._write_config_file')
    @mock.patch('cumulus.starcluster.tasks.celery.monitor.Task.retry')
    def test_monitor_job_running(self, retry, _write_config_file, *args):
        _write_config_file.return_value = 'dummy file path'

        job_id = 'dummy'
        cluster = {
            'name': 'dummy',
            'configId': 'dummy'
        }
        job_model = {
            '_id': job_id,
            'sgeId': '1',
            'name': 'dummy',
            'output': []
        }

        MockMaster.execute_stack = [[ 'job-ID  prior   name       user         state submit/start at     queue  slots ja-task-ID',
                             '-----------------------------------------------------------------------------------------',
                             '1 0.00000 hostname   sgeadmin     r     09/09/2009 14:58:14                1']]

        def _get_status(url, request):
            content = {
                'status': 'running'
            }
            content = json.dumps(content)
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            self._get_status_called  = True
            return httmock.response(200, content, headers, request=request)

        def _set_status(url, request):
            expected = {'status': 'running', 'output': [], 'timings': {}}
            self._set_status_called = json.loads(request.body) == expected

            self.assertEqual(json.loads(request.body), expected, 'Unexpected status update body')
            return httmock.response(200, None, {}, request=request)

        status_url = '/api/v1/jobs/%s/status' % job_id
        get_status = httmock.urlmatch(
            path=r'^%s$' % status_url, method='GET')(_get_status)

        status_update_url = '/api/v1/jobs/%s' % job_id
        set_status = httmock.urlmatch(
            path=r'^%s$' % status_update_url, method='PATCH')(_set_status)

        with httmock.HTTMock(get_status, set_status):
            job.monitor_job(cluster, job_model, **{'girder_token': 's', 'log_write_url': 1})

        self.assertTrue(self._get_status_called, 'Expect get status endpoint to be hit')
        self.assertTrue(self._set_status_called, 'Expect set status endpoint to be hit')

    @mock.patch('starcluster.config.StarClusterConfig', new=MockStarClusterConfig)
    @mock.patch('starcluster.logger')
    @mock.patch('cumulus.starcluster.logging')
    @mock.patch('cumulus.starcluster.tasks.common._write_config_file')
    @mock.patch('cumulus.starcluster.tasks.celery.monitor.Task.retry')
    def test_monitor_job_queued(self, retry, _write_config_file, *args):
        _write_config_file.return_value = 'dummy file path'

        job_id = 'dummy'
        cluster = {
            'name': 'dummy',
            'configId': 'dummy'
        }
        job_model = {
            '_id': job_id,
            'sgeId': '1',
            'name': 'dummy',
            'output': []
        }

        MockMaster.execute_stack = [[ 'job-ID  prior   name       user         state submit/start at     queue  slots ja-task-ID',
                             '-----------------------------------------------------------------------------------------',
                             '1 0.00000 hostname   sgeadmin     q     09/09/2009 14:58:14                1']]

        def _get_status(url, request):
            content = {
                'status': 'queued'
            }
            content = json.dumps(content)
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            self._get_status_called  = True
            return httmock.response(200, content, headers, request=request)

        def _set_status(url, request):
            expected = {'status': 'queued', 'output': [], 'timings': {}}
            self._set_status_called = json.loads(request.body) == expected

            self.assertEqual(json.loads(request.body), expected, 'Unexpected status update body')
            return httmock.response(200, None, {}, request=request)

        status_url = '/api/v1/jobs/%s/status' % job_id
        get_status = httmock.urlmatch(
            path=r'^%s$' % status_url, method='GET')(_get_status)

        status_update_url = '/api/v1/jobs/%s' % job_id
        set_status = httmock.urlmatch(
            path=r'^%s$' % status_update_url, method='PATCH')(_set_status)

        with httmock.HTTMock(get_status, set_status):
            job.monitor_job(cluster, job_model, **{'girder_token': 's', 'log_write_url': 1})

        self.assertTrue(self._get_status_called, 'Expect get status endpoint to be hit')
        self.assertTrue(self._set_status_called, 'Expect set status endpoint to be hit')

    @mock.patch('starcluster.config.StarClusterConfig', new=MockStarClusterConfig)
    @mock.patch('starcluster.logger')
    @mock.patch('cumulus.starcluster.logging')
    @mock.patch('cumulus.starcluster.tasks.common._write_config_file')
    @mock.patch('cumulus.starcluster.tasks.celery.monitor.Task.retry')
    def test_monitor_job_tail_output(self, retry, _write_config_file, *args):
        _write_config_file.return_value = 'dummy file path'

        job_id = 'dummy'
        cluster = {
            'name': 'dummy',
            'configId': 'dummy'
        }
        job_model = {
            '_id': job_id,
            'sgeId': '1',
            'name': 'dummy',
            'output': [{'tail': True,  'path': 'dummy/file/path'}]
        }

        MockMaster.execute_stack = [[ 'job-ID  prior   name       user         state submit/start at     queue  slots ja-task-ID',
                             '-----------------------------------------------------------------------------------------',
                             '1 0.00000 hostname   sgeadmin     r     09/09/2009 14:58:14                1'], ['i have a tail', 'asdfas'] ]

        def _get_status(url, request):
            content = {
                'status': 'running'
            }
            content = json.dumps(content)
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            self._get_status_called  = True
            return httmock.response(200, content, headers, request=request)

        def _set_status(url, request):
            expected = {u'status': u'running', u'output': [{u'content': [u'i have a tail', u'asdfas'], u'path': u'dummy/file/path', u'tail': True}], u'timings': {}}
            self._set_status_called = json.loads(request.body) == expected

            self.assertEqual(json.loads(request.body), expected, 'Unexpected status update body')
            return httmock.response(200, None, {}, request=request)

        status_url = '/api/v1/jobs/%s/status' % job_id
        get_status = httmock.urlmatch(
            path=r'^%s$' % status_url, method='GET')(_get_status)

        status_update_url = '/api/v1/jobs/%s' % job_id
        set_status = httmock.urlmatch(
            path=r'^%s$' % status_update_url, method='PATCH')(_set_status)

        with httmock.HTTMock(get_status, set_status):
            job.monitor_job(cluster, job_model, **{'girder_token': 's', 'log_write_url': 1})

        self.assertTrue(self._get_status_called, 'Expect get status endpoint to be hit')
        self.assertTrue(self._set_status_called, 'Expect set status endpoint to be hit')

