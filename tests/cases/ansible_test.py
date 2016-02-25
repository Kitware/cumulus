import unittest
import mock
import os
import ansible
from cumulus.ansible.tasks.cluster import run_playbook

class AnsibleTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def assertAnsibleReturn(self, results, ok, changed, failures, skipped):
        self.assertEqual(results['localhost']['ok'], ok)
        self.assertEqual(results['localhost']['changed'], changed)
        self.assertEqual(results['localhost']['failures'], failures)
        self.assertEqual(results['localhost']['skipped'], skipped)

    def test_run_playbook(self):

        path = os.path.join(os.environ["CUMULUS_SOURCE_DIRECTORY"],
                            'tests', 'cases', 'fixtures', 'ansible',
                            'test_run_playbook.yml')
        inventory = ansible.inventory.Inventory(['localhost'])

        results = run_playbook(path, inventory)
        self.assertAnsibleReturn(results, 1, 0, 0, 0)
