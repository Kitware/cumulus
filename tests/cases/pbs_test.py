import unittest
import mock

from cumulus.queue import get_queue_adapter
from cumulus.queue.abstract import AbstractQueueAdapter
from cumulus.constants import QueueType

class SgeQueueAdapterTestCase(unittest.TestCase):

    def setUp(self):
        self._cluster_connection = mock.MagicMock()
        self._adapter = get_queue_adapter({
            'queue': {
                'system': QueueType.PBS
            }
        }, self._cluster_connection)


    def test_terminate_job(self):
        job_id = 123
        job = {
            self._adapter.QUEUE_JOB_ID: job_id
        }

        self._adapter.terminate_job(job)
        expected_call = [mock.call('qdel %d' % job_id)]
        self.assertEqual(self._cluster_connection.execute.call_args_list, expected_call)

    def test_submit_job(self):
        job_id = '123'
        test_output = ['%s.ulex.kitware.com' % job_id]
        job_script = 'script.sh'
        job = {
            AbstractQueueAdapter.QUEUE_JOB_ID: job_id,
            'dir': '/tmp'
        }
        expected_calls = [mock.call('cd /tmp && qsub ./%s' % job_script)]

        self._cluster_connection.execute.return_value = test_output
        actual_job_id = self._adapter.submit_job(job, job_script)
        self.assertEqual(self._cluster_connection.execute.call_args_list, expected_calls)
        self.assertEqual(actual_job_id, job_id)

        test_output = ['Your fook %s ("test.sh") has been submitted' % job_id]
        self._cluster_connection.execute.return_value = test_output
        with self.assertRaises(Exception) as cm:
            self._adapter.submit_job(test_output)

        self.assertIsNotNone(cm.exception)


    def test_job_status(self):
        job_id = '1126'
        job = {
            AbstractQueueAdapter.QUEUE_JOB_ID: job_id
        }
        job_status_output = [
            'Job id                    Name             User            Time Use S Queue',
            '------------------------- ---------------- --------------- -------- - -----',
            '%s.ulex                    sleep.sh         cjh             00:00:00 C batch' % job_id,
            '2.ulex                    sleep.sh         cjh             00:00:00 C batch'
        ]
        expected_calls = [mock.call('qstat')]
        self._cluster_connection.execute.return_value = job_status_output
        status = self._adapter.job_status(job)
        self.assertEqual(self._cluster_connection.execute.call_args_list, expected_calls)
        self.assertEqual(status, 'complete')

    def test_unsupported(self):
        with self.assertRaises(Exception) as cm:
            get_queue_adapter({
                'queue': {
                    'system': 'foo'
                }
            }, None)

        self.assertIsNotNone(cm.exception)
