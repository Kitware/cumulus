import unittest
import os
from cumulus.ansible.tasks.providers import CloudProvider

class CloudProviderTestCase(unittest.TestCase):
    def setup(self):
        pass

    def tearDown(self):
        pass


    def test_empty_profile(self):
        p = CloudProvider({})


#    p = CloudProvider({
#        "accessKeyId": os.environ.get("AWS_ACCESS_KEY_ID"),
#        "secretAccessKey": os.environ.get("AWS_SECRET_ACCESS_KEY"),
#        "type": "ec2"
#    })
