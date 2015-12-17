#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2015 Kitware Inc.
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
import mock

from cumulus.queue import get_queue_adapter
from cumulus.queue.abstract import AbstractQueueAdapter
from cumulus.constants import QueueType

class SgeQueueAdapterTestCase(unittest.TestCase):

    def setUp(self):
        self._cluster_connection = mock.MagicMock()
        self._adapter = get_queue_adapter({
            'config': {
                'scheduler': {
                    'type': QueueType.SGE
                }
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
        test_output = ['Your job %s ("test.sh") has been submitted' % job_id]
        job_script = 'script.sh'
        job = {
            AbstractQueueAdapter.QUEUE_JOB_ID: job_id,
            'dir': '/tmp'
        }
        expected_calls = [mock.call('cd /tmp && qsub -cwd ./%s' % job_script)]

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
            'job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID',
            '-----------------------------------------------------------------------------------------------------------------',
            '1126 0.50000 test.sh    cjh          r     11/18/2015 13:18:09 main.q@ulmus.kitware.com           1'
        ]
        expected_calls = [mock.call('qstat')]
        self._cluster_connection.execute.return_value = job_status_output
        status = self._adapter.job_status(job)
        self.assertEqual(self._cluster_connection.execute.call_args_list, expected_calls)
        self.assertEqual(status, 'running')

    def test_unsupported(self):
        with self.assertRaises(Exception) as cm:
            get_queue_adapter({
                'config': {
                    'scheduler': {
                        'type': 'foo'
                    }
                }
            }, None)

        self.assertIsNotNone(cm.exception)
