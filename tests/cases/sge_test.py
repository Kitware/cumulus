import unittest
import httmock
import os
import json
from jsonpath_rw import parse
import mock

from cumulus.queue import get_queue_adapter
from cumulus.constants import QueueType

class SgeQueueAdapterTestCase(unittest.TestCase):

    def setUp(self):
        self._adapter = get_queue_adapter({
            'queue': {
                'system': QueueType.SGE
            }
        })

    def test_parse_job_id(self):
        test_id = '1121'
        test_output = ['Your job %s ("test.sh") has been submitted' % test_id]

        self.assertEqual(self._adapter.parse_job_id(test_output), test_id)

        test_output = ['Your fook %s ("test.sh") has been submitted' % test_id]
        with self.assertRaises(Exception) as cm:
            self._adapter.parse_job_id(test_output)

        self.assertIsNotNone(cm.exception)

    def test_terminate_job_command(self):
        job_id = 123
        job = {
            self._adapter.QUEUE_JOB_ID: job_id
        }
        expected = 'qdel %d' % job_id
        actual = self._adapter.terminate_job_command(job)
        self.assertEqual(actual, expected)

    def test_submit_job_command(self):
        job_script = '/foo/bar/script.sh'
        expected = 'qsub -cwd ./%s' % job_script
        actual = self._adapter.submit_job_command(job_script)
        self.assertEqual(actual, expected)

    def test_job_status_command(self):
        job_id = 123
        job = {
            self._adapter.QUEUE_JOB_ID: job_id
        }
        expected = 'qstat'
        actual = self._adapter.job_status_command(job)
        self.assertEqual(actual, expected)

    def test_unsupported(self):
        with self.assertRaises(Exception) as cm:
            get_queue_adapter({
                'queue': {
                    'system': 'foo'
                }
            })

        self.assertIsNotNone(cm.exception)

    def test_extract_job_status(self):
        job_id = '1126'
        job = {
               self._adapter.QUEUE_JOB_ID: job_id
        }
        job_status_output = [
            'job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID',
            '-----------------------------------------------------------------------------------------------------------------',
            '1126 0.50000 test.sh    cjh          r     11/18/2015 13:18:09 main.q@ulmus.kitware.com           1'
        ]
        status = self._adapter.extract_job_status(job_status_output, job)

        self.assertEqual(status, 'r')
