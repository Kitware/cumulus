#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2016 Kitware Inc.
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
import argparse
import unittest
import json
import logging
import time


from ansible_integration_test import AnsibleIntegrationTest
from girder_client import HttpError

class EC2VolumeIntegrationTest(AnsibleIntegrationTest):

    def __init__(self, name, girder_url, girder_user, girder_password, aws_access_key_id,
                  aws_secret_access_key):
        super(EC2VolumeIntegrationTest, self).__init__(
            name, girder_url, girder_user, girder_password, aws_access_key_id,
            aws_secret_access_key)
        self._volume_id = None
        self._profile_id = None
        self._cluster_id = None

    def tearDown(self):
        super(EC2VolumeIntegrationTest, self).tearDown()
        if self._volume_id:
            # Try to detach to volume
            try:
                volume_url = 'volumes/%s/detach' % self._volume_id
                r = self._client.put(volume_url)

                volume_status_url = 'volumes/%s/status' % self._volume_id
                self._wait_for_status(volume_status_url, 'available', timeout=60)

            except HttpError as error:
                # if it doesn't exist continue
                if error.status != 400:
                    raise error

            # Try to delete the volume
            try:
                volume_url = 'volumes/%s' % self._volume_id
                r = self._client.delete(volume_url)
            except HttpError as error:
                # if it doesn't exist continue
                if error.status != 400:
                    raise error



    def create_volume(self):
        volume_url = 'volumes'
        body =  {
            "name": "testVolume",
            "profileId": self._profile_id,
            "size": 11,
            "type": "ebs",
            "zone": "us-west-2a"
        }

        r = self._client.post(volume_url, data=json.dumps(body))
        self._volume_id = r['_id']

    def attach_volume(self):
        attach_url = 'volumes/%s/clusters/%s/attach' % (self._volume_id, self._cluster_id)
        body = {
            'path': '/mnt/data/'
        }

        r = self._client.put(attach_url, data=json.dumps(body))

        volume_status_url = 'volumes/%s/status' % self._volume_id
        self._wait_for_status(volume_status_url, 'in-use', timeout=60)


    def detach_volume(self):
        volume_url = 'volumes/%s/detach' % self._volume_id
        r = self._client.put(volume_url)

        volume_status_url = 'volumes/%s/status' % self._volume_id
        self._wait_for_status(volume_status_url, 'available', timeout=60)



    def delete_volume(self):
        volume_url = 'volumes/%s' % self._volume_id
        r = self._client.delete(volume_url)

        self.assertEquals(r['status'], 'deleting')

        start = time.time();
        while True:
            time.sleep(1)
            try:
                r = self._client.get("/volumes/%s" % self._volume_id)
            except HttpError as error:
                if error.status == 400:
                    break
                else:
                    raise error

            if time.time() - start > 60:
                self.fail('Volume never moved out of status: %s' % r['ec2']['status'])





    def test(self):
        try:
            t0 = time.time()
            self.create_profile()
            logging.info("Finished creating profile (%s)" % (time.time() - t0)); t0 = time.time()
            self.create_cluster()
            logging.info("Finished creating cluster (%s)" % (time.time() - t0)); t0 = time.time()
            self.launch_cluster()
            logging.info("Finished launching cluster (%s)" % (time.time() - t0)); t0 = time.time()

            self.create_volume()
            logging.info("Finished creating volume (%s)" % (time.time() - t0)); t0 = time.time()

            self.attach_volume()
            logging.info("Finished attaching volume (%s)" % (time.time() - t0)); t0 = time.time()

            self.detach_volume()
            logging.info("Finished detaching volume (%s)" % (time.time() - t0)); t0 = time.time()

            self.delete_volume()
            logging.info("Finished deleting volume (%s)" % (time.time() - t0)); t0 = time.time()

            self.terminate_cluster()
            logging.info("Finished terminating cluster (%s)" % (time.time() - t0)); t0 = time.time()
        except HttpError as error:
            self.fail(error.responseText)
        except KeyboardInterrupt:
            self.fail("Interupted!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(parents=[AnsibleIntegrationTest.parser])
    parser.add_argument('-i', '--aws_access_key_id', help='', required=True)
    parser.add_argument('-a', '--aws_secret_access_key', help='', required=True)
    parser.add_argument('-l', '--loglevel', help='', required=False, default="INFO",
                        choices=["INFO", "WARNING", "ERROR", "DEBUG"])

    args = parser.parse_args()

    logging.basicConfig(level=(getattr(logging, args.loglevel)))
    logging.getLogger("requests").setLevel(logging.WARNING)

    suite = unittest.TestSuite()
    suite.addTest(EC2VolumeIntegrationTest('test', args.girder_url, args.girder_user,
        args.girder_password, args.aws_access_key_id, args.aws_secret_access_key))
    unittest.TextTestRunner().run(suite)
