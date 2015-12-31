import unittest
import mock
import os

from cumulus.queue import get_queue_adapter
from cumulus.queue.abstract import AbstractQueueAdapter
from cumulus.constants import QueueType
from cumulus.starcluster.tasks import job

class PbsQueueAdapterTestCase(unittest.TestCase):

    def setUp(self):
        self._cluster_connection = mock.MagicMock()
        self._adapter = get_queue_adapter({
            'config': {
                'scheduler': {
                    'type': QueueType.PBS
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

    def test_submission_template_pbs(self):
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
                    'type': 'pbs'
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
                             'pbs_submission_script1.sh')

        with open(path, 'r') as fp:
            expected = fp.read()

        script = job._generate_submission_script(job_model, cluster, {})
        self.assertEqual(script, expected)

        path = os.path.join(os.environ["CUMULUS_SOURCE_DIRECTORY"],
                            'tests', 'cases', 'fixtures', 'job',
                            'pbs_submission_script2.sh')

        with open(path, 'r') as fp:
            expected = fp.read()

        job_params = {
            'numberOfSlots': 12312312
        }
        script = job._generate_submission_script(job_model, cluster, job_params)
        self.assertEqual(script, expected)


    def test_submission_template_pbs_nodes(self):
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
                    'type': 'pbs'
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

        # Just nodes specfied

        path = os.path.join(os.environ["CUMULUS_SOURCE_DIRECTORY"],
                            'tests', 'cases', 'fixtures', 'job',
                            'pbs_submission_script_nodes.sh')

        with open(path, 'r') as fp:
            expected = fp.read()

        job_params = {
            'numberOfNodes': 12312312
        }
        script = job._generate_submission_script(job_model, cluster, job_params)
        self.assertEqual(script, expected)

        # Nodes with number of cores
        path = os.path.join(os.environ["CUMULUS_SOURCE_DIRECTORY"],
                            'tests', 'cases', 'fixtures', 'job',
                             'pbs_submission_script_nodes_cores.sh')

        with open(path, 'r') as fp:
            expected = fp.read()

        job_params = {
            'numberOfNodes': 12312312,
            'numberOfCoresPerNode': 8
        }
        script = job._generate_submission_script(job_model, cluster, job_params)
        self.assertEqual(script, expected)

        # Nodes with number of gpus
        path = os.path.join(os.environ["CUMULUS_SOURCE_DIRECTORY"],
                            'tests', 'cases', 'fixtures', 'job',
                             'pbs_submission_script_nodes_gpus.sh')

        with open(path, 'r') as fp:
            expected = fp.read()

        job_params = {
            'numberOfNodes': 12312312,
            'numberOfGpusPerNode': 8
        }
        script = job._generate_submission_script(job_model, cluster, job_params)
        self.assertEqual(script, expected)

        # Nodes with number of cores and gpus
        path = os.path.join(os.environ["CUMULUS_SOURCE_DIRECTORY"],
                             'tests', 'cases', 'fixtures', 'job',
                             'pbs_submission_script_nodes_cores_gpus.sh')

        with open(path, 'r') as fp:
            expected = fp.read()

        job_params = {
            'numberOfNodes': 12312312,
            'numberOfGpusPerNode': 8,
            'numberOfCoresPerNode': 8
        }
        script = job._generate_submission_script(job_model, cluster, job_params)
        self.assertEqual(script, expected)
