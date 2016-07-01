import unittest
import os
from cumulus.ansible.tasks.providers import CloudProvider, EC2Provider

class CloudProviderTestCase(unittest.TestCase):
    def setup(self):
        pass

    def tearDown(self):
        pass

    def test_empty_profile(self):
        with self.assertRaises(AssertionError) as context:
            p = CloudProvider({})

        self.assertTrue('Profile does not have a "cloudProvider" attribute'
                        in str(context.exception))

    def test_ec2_profile(self):
        p = CloudProvider({'cloudProvider': 'ec2'})
        self.assertTrue(isinstance(p, EC2Provider))
