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
import os

from cumulus.queue import get_queue_adapter
from cumulus.queue.abstract import AbstractQueueAdapter
from cumulus.constants import QueueType
from cumulus.tasks import job


class SgeQueueAdapterTestCase(unittest.TestCase):

    def setUp(self):
        self._cluster_connection = mock.MagicMock()
        self._adapter = get_queue_adapter({
            'config': {
                'scheduler': {
                    'type': QueueType.SGE
                }
            },
            'type': 'trad'
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


    def test_job_statuses(self):
        job1_id = '1126'
        job1 = {
            AbstractQueueAdapter.QUEUE_JOB_ID: job1_id
        }
        job2_id = '1127'
        job2 = {
            AbstractQueueAdapter.QUEUE_JOB_ID: job2_id
        }
        job_status_output = [
            'job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID',
            '-----------------------------------------------------------------------------------------------------------------',
            '%s 0.50000 test.sh    cjh          r     11/18/2015 13:18:09 main.q@ulmus.kitware.com           1' % job1_id,
            '%s 0.50000 test.sh    cjh          q     11/18/2015 13:18:09 main.q@ulmus.kitware.com           1' % job2_id
        ]
        expected_calls = [mock.call('qstat')]
        self._cluster_connection.execute.return_value = job_status_output
        status = self._adapter.job_statuses([job1])
        self.assertEqual(self._cluster_connection.execute.call_args_list, expected_calls)
        self.assertEqual(status[0][1], 'running')

        # Now try two jobs
        self._cluster_connection.reset_mock()
        expected_calls = [mock.call('qstat')]
        self._cluster_connection.execute.return_value = job_status_output
        status = self._adapter.job_statuses([job1, job2])
        self.assertEqual(self._cluster_connection.execute.call_args_list, expected_calls)
        self.assertEqual(status[0][1], 'running')
        self.assertEqual(status[1][1], 'queued')

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

    def test_submission_template_sge(self):
        cluster = {
            '_id': 'dummy',
            'type': 'trad',
            'name': 'dummy',
            'config': {
                'host': 'dummy',
                'ssh': {
                    'user': 'dummy',
                    'passphrase': 'its a secret'
                },
                'scheduler': {
                    'type': 'sge'
                }
            }
        }
        job_id = '123432423'
        job_model = {
            '_id': job_id,
            'queueJobId': '1',
            'name': 'dummy',
            'commands': ['ls', 'sleep 20', 'mpirun -n 1000000 parallel'],
            'output': [{'tail': True,  'path': 'dummy/file/path'}]
        }

        path = os.path.join(os.environ["CUMULUS_SOURCE_DIRECTORY"],
                            'tests', 'cases', 'fixtures', 'job',
                            'sge_submission_script1.sh')

        with open(path, 'r') as fp:
            expected = fp.read()

        script = job._generate_submission_script(job_model, cluster, {})
        self.assertEqual(script, expected)

        path = os.path.join(os.environ["CUMULUS_SOURCE_DIRECTORY"],
                            'tests', 'cases', 'fixtures', 'job',
                            'sge_submission_script2.sh')

        with open(path, 'r') as fp:
            expected = fp.read()

        job_params = {
            'parallelEnvironment': 'big',
            'numberOfSlots': 12312312
        }
        script = job._generate_submission_script(job_model, cluster, job_params)
        self.assertEqual(script, expected)

    def test_submission_template_sge_gpus(self):
        cluster = {
            '_id': 'dummy',
            'type': 'trad',
            'name': 'dummy',
            'config': {
                'host': 'dummy',
                'ssh': {
                    'user': 'dummy',
                    'passphrase': 'its a secret'
                },
                'scheduler': {
                    'type': 'sge'
                }
            }
        }
        job_id = '123432423'
        job_model = {
            '_id': job_id,
            'queueJobId': '1',
            'name': 'dummy',
            'commands': ['ls', 'sleep 20', 'mpirun -n 1000000 parallel'],
            'output': [{'tail': True,  'path': 'dummy/file/path'}]
        }

        path = os.path.join(os.environ["CUMULUS_SOURCE_DIRECTORY"],
                            'tests', 'cases', 'fixtures', 'job',
                            'sge_submission_script_gpus.sh')

        with open(path, 'r') as fp:
            expected = fp.read()

        job_params = {
            'gpus': 2
        }
        script = job._generate_submission_script(job_model, cluster, job_params)
        self.assertEqual(script, expected)
