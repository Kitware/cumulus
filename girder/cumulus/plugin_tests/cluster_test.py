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

from tests import base
import json
import mock
from bson.objectid import ObjectId

import cumulus
from cumulus.transport.files import get_assetstore_url_base
from cumulus.testing import AssertCallsMixin

from girder.utility.model_importer import ModelImporter

def setUpModule():
    base.enabledPlugins.append('cumulus')
    base.enabledPlugins.append('sftp')
    base.startServer()


def tearDownModule():
    base.stopServer()


class ClusterTestCase(AssertCallsMixin, base.TestCase):
    def setUp(self):
        super(ClusterTestCase, self).setUp()

        users = ({
            'email': 'cumulus@email.com',
            'login': 'cumulus',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        }, {
            'email': 'regularuser@email.com',
            'login': 'regularuser',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        }, {
            'email': 'another@email.com',
            'login': 'another',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        })
        self._cumulus, self._user, self._another_user = \
            [ModelImporter.model('user').createUser(**user) for user in users]

        self._group = ModelImporter.model('group').createGroup('cumulus', self._cumulus)

        self._valid_key = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDJ0wahxwaNbCDdbRll9FypQRXQv5PXQSTh1IeSynTcZZWSyQH4JhoI0lb3/IW7GllIkWblEuyv2SHzXMKRaaFuwmnU1zsY6Y55N6DJt0e9TvieT8MfaM2e7qqaN+0RS2aFb8iw3i+G80tmFVJWuNm7AITVVPf60Nbc5Bgk9qVIa4BakJ3SmW0p/iHT3CStb/k+psevFYyYCEw5l3+3ejPh9b/3423yRzq5r0cyOw8y8fIe4JV8MlE4z2huc/o9Xpw8mzNim7QdobNOylwJsvIYtB4d+MTqvsnt16e22BS/FKuTXx6jGRFFtYNWwwDQe9IIxYb6dPs1XPKVx081nRUwNjar2um41XUOhPx1N5+LfbrYkACVEZiEkW/Ph6hu0PsYQXbL00sWzrzIunixepn5c2dMnDvugvGQA54Z0EXgIYHnetJp2Xck1pJH6oNSSyA+5Mx5QAH5MFNL3YOnGxGBLrkUfK9Ff7QOiZdqXbZoXXS49WtL42Jsv8SgFu3w5NLffvD6/vCOBHwWxh+8VLg5n28M7pZ8+xyMBidkGkG9di2PfV4XsSAeoIc5utgbUJFT6URr2pW9KT4FxTq/easgiJFZUz48SNAjcBneElB9bjAaGf47BPfCNsIAWU2c9MZJWjURpWtzfk21k2/BAfBPs2VNb8dapY6dNinxLqbPIQ== your_email@example.com'

        # Create a dummy profile
        self._user_profile = {
            'status' : 'available',
            'secretAccessKey' : 'secret',
            'availabilityZone' : 'us-west-1a',
            'name' : 'test',
            'regionHost' : 'ec2.us-west-1.amazonaws.com',
            'userId' : ObjectId(self._user['_id']),
            'accessKeyId' : 'id',
            'publicIPs' : False,
            'regionName' : 'us-west-1'
        }
        self._user_profile = ModelImporter.model('aws', 'cumulus').save(
            self._user_profile, validate=False)

        self._another_user_profile = {
            'status' : 'available',
            'secretAccessKey' : 'secret',
            'availabilityZone' : 'us-west-1a',
            'name' : 'test',
            'regionHost' : 'ec2.us-west-1.amazonaws.com',
            'userId' : ObjectId(self._another_user['_id']),
            'accessKeyId' : 'id',
            'publicIPs' : False,
            'regionName' : 'us-west-1'
        }
        self._another_user_profile = ModelImporter.model('aws', 'cumulus').save(
            self._another_user_profile, validate=False)


    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_create(self, generate_key_pair):
        body = {
            'config': [
                {
                    '_id': ''
                }
            ],
            'name': ''
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)

        body['config'] = {
        }
        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)

        body['profileId'] = str(self._user_profile['_id'])
        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)

        body['name'] = 'mycluster'
        json_body = json.dumps(body)
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)

        # Try creating with the same name
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)

        # Try creating with the same name as another user, this should work
        body['profileId'] = str(self._another_user_profile['_id'])
        json_body = json.dumps(body)
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._another_user)
        self.assertStatus(r, 201)

        # Try creating trad cluster with the same name, this should also work
        trad_body = {
            'config': {
                'host': 'myhost',
                'ssh': {
                    'user': 'myuser'
                }
            },
            'name': 'mycluster',
            'type': 'trad'
        }
        json_body = json.dumps(trad_body)
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)

    @mock.patch('cumulus.aws.ec2.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus_plugin.models.aws.get_ec2_client')
    def test_create_using_aws_profile(self, get_ec2_client, generate_key_pair):
        # First create a profile
        region_host = 'cornwall.ec2.amazon.com'
        ec2_client = get_ec2_client.return_value
        ec2_client.describe_regions.return_value = {
            'Regions': [{
                'RegionName': 'cornwall',
                'Endpoint': region_host
                }]
        }

        body = {
            'name': 'myprof',
            'accessKeyId': 'mykeyId',
            'secretAccessKey': 'mysecret',
            'regionName': 'cornwall',
            'availabilityZone': 'cornwall-2b'
        }

        create_url = '/user/%s/aws/profiles' % str(self._user['_id'])
        r = self.request(create_url, method='POST',
                         type='application/json', body=json.dumps(body),
                         user=self._user)
        self.assertStatus(r, 201)
        profile_id = str(r.json['_id'])

        # First test invalid profileId
        body = {
            'profileId': '546a1844ff34c70456111385',
            'name': 'mycluster'
        }

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json.dumps(body), user=self._user)
        self.assertStatus(r, 400)

        # Now test with valid profile
        body = {
            'profileId': profile_id,
            'name': 'mycluster'
        }

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json.dumps(body), user=self._user)
        self.assertStatus(r, 201)

        # Test that we can provide custom configuration and it will be added
        # to the cluster.
        job_dir = '/test'
        body = {
            'profileId': profile_id,
            'name': 'myconfigcluster',
            'config': {
                'jobOutputDir': job_dir
            }
        }

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json.dumps(body), user=self._user)
        self.assertStatus(r, 201)
        cluster = r.json
        self.assertTrue('config' in cluster)
        self.assertTrue('jobOutputDir' in cluster['config'])
        self.assertEqual(cluster['config']['jobOutputDir'], job_dir)


    def test_get(self):
        body = {
            'profileId': str(self._user_profile['_id']),
            'name': 'test'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        r = self.request('/clusters/%s' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)

        self.assertEqual(r.json['_id'], cluster_id)

        # Check for 404
        r = self.request('/clusters/546a1844ff34c70456111185', method='GET',
                         user=self._user)
        self.assertStatus(r, 404)

    def test_update(self):
        status_body = {
            'status': 'terminating'
        }

        r = self.request(
            '/clusters/546a1844ff34c70456111185', method='PATCH',
            type='application/json', body=json.dumps(status_body),
            user=self._cumulus)

        self.assertStatus(r, 404)

        body = {
            'profileId': str(self._user_profile['_id']),
            'name': 'test'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        r = self.request(
            '/clusters/%s' % str(cluster_id), method='PATCH',
            type='application/json', body=json.dumps(status_body),
            user=self._cumulus)

        self.assertStatusOk(r)
        expected_cluster = {
            u'_id': cluster_id,
            u'config': {
                u'scheduler': {
                    u'type': u'sge'
                },
                u'ssh': {
                    u'user': u'ubuntu',
                    u'key': str(self._user_profile['_id'])
                },
                u'launch': {
                    u'spec': u'default',
                    u'params': { }
                }
            },
            u'name': u'test',
            u'profileId': str(self._user_profile['_id']),
            u'status': u'terminating',
            u'type': u'ec2',
            u'userId': str(self._user['_id'])
        }
        self.assertEqual(r.json, expected_cluster)

        # Check we get the right server side events
        r = self.request('/notification/stream', method='GET', user=self._user,
                         isJson=False, params={'timeout': 0})
        self.assertStatusOk(r)
        notifications = self.getSseMessages(r)
        self.assertEqual(len(notifications), 2, 'Expecting two notifications')
        notification = notifications[1]
        notification_type = notification['type']
        data = notification['data']
        self.assertEqual(notification_type, 'cluster.status')
        expected = {
            u'status': u'terminating',
            u'_id': cluster_id
        }
        self.assertEqual(data, expected, 'Unexpected notification data')


        # Test GET status
        r = self.request('/clusters/%s' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        expected_status =  {
            u'_id': cluster_id,
            u'config': {
                u'scheduler': {
                    u'type': u'sge'
                },
                u'ssh': {
                    u'user': u'ubuntu',
                    u'key': str(self._user_profile['_id'])
                },
                u'launch': {
                    u'spec': u'default',
                    u'params': { }
                }
            },
            u'name': u'test',
            u'profileId': str(self._user_profile['_id']),
            u'status': 'terminating',
            u'type': u'ec2',
            u'userId': str(self._user['_id'])
        }

        self.assertEqual(r.json, expected_status)

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_update_traditional(self, generate_key):
        body = {
            'config': {
                'host': 'myhost',
                'ssh': {
                    'user': 'myuser'
                }
            },
            'name': 'test',
            'type': 'trad'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        update_body = {
            'config': {
                'ssh': {
                    'publicKey': self._valid_key,
                    'passphrase': 'supersecret'
                }
            }
        }
        r = self.request(
            '/clusters/%s' % str(cluster_id), method='PATCH',
            type='application/json', body=json.dumps(update_body),
            user=self._cumulus)

        self.assertStatusOk(r)
        expected = {u'status': u'creating',
                    u'userId': str(self._user['_id']),
                    u'type': u'trad',
                    u'_id': cluster_id,
                    u'config': {
                        u'scheduler': {
                            u'type': u'sge'
                        },
                        u'host': u'myhost',
                        u'ssh': {
                            u'user': u'myuser',
                            u'key': cluster_id,
                            u'publicKey': self._valid_key}
                    }, u'name': u'test'
                }

        self.assertEqual(
            self.normalize(expected), self.normalize(r.json), 'Unexpected response')

        r = self.request('/clusters/%s' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        expected = {u'status': u'creating',
                    u'userId': str(self._user['_id']),
                    u'type': u'trad',
                    u'_id': cluster_id,
                    u'config': {
                        u'scheduler': {
                            u'type': u'sge'
                        },
                        u'host': u'myhost',
                        u'ssh': {
                            u'user': u'myuser',
                            u'key': cluster_id,
                            u'publicKey': self._valid_key
                        }
                    },
                    u'name': u'test'
        }
        self.assertEqual(
            self.normalize(expected), self.normalize(r.json), 'Unexpected response')

        # Check that if we are in the right group we will get the passphrase
        r = self.request('/clusters/%s' % str(cluster_id), method='GET',
                         user=self._cumulus)
        self.assertStatusOk(r)
        expected = {u'status': u'creating', u'userId': str(self._user['_id']), u'type': u'trad', u'_id': cluster_id, u'config': {u'scheduler': {u'type': u'sge'}, u'host': u'myhost', u'ssh': {
            u'user': u'myuser', u'key': cluster_id, u'publicKey': self._valid_key, u'passphrase': u'supersecret'}}, u'name': u'test'}
        self.assertEqual(
            self.normalize(expected), self.normalize(r.json), 'Unexpected response')

        # Check we get an error if we try and update in invalid key
        update_body = {
            'config': {
                'ssh': {
                    'publicKey': 'bogus',
                    'passphrase': 'supersecret'
                }
            }
        }
        r = self.request(
            '/clusters/%s' % str(cluster_id), method='PATCH',
            type='application/json', body=json.dumps(update_body),
            user=self._cumulus)
        self.assertStatus(r, 400)

    def test_log(self):
        body = {
            'profileId': str(self._user_profile['_id']),
            'name': 'test'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        log_entry = {
            'msg': 'Some message'
        }

        r = self.request('/clusters/546a1844ff34c70456111185/log', method='GET',
                         user=self._user)
        self.assertStatus(r, 404)

        r = self.request('/clusters/%s/log' % str(cluster_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        r = self.request('/clusters/%s/log' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        expected_log = {u'log': [{u'msg': u'Some message'}]}
        self.assertEqual(r.json, expected_log)

        r = self.request('/clusters/%s/log' % str(cluster_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        r = self.request('/clusters/%s/log' % str(cluster_id), method='GET',
                         user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json['log']), 2)

        r = self.request('/clusters/%s/log' % str(cluster_id), method='GET',
                         params={'offset': 1}, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json['log']), 1)

    @mock.patch('cumulus.ansible.tasks.cluster.start_cluster.delay')
    def test_start(self, start_cluster):

        body = {
            'name': 'test',
            'profileId': str(self._user_profile['_id'])
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        r = self.request('/clusters/%s/start' % str(cluster_id), method='PUT',
                         type='application/json', body=json_body, user=self._user)

        self.assertEqual(len(start_cluster.call_args_list), 1)


    @mock.patch('cumulus.tasks.job.submit')
    def test_submit_job(self, submit):
        body = {
            'profileId': str(self._user_profile['_id']),
            'name': 'test'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        # Create a job
        body = {
            'onComplete': {
                'cluster': 'terminate'
            },
            'input': [
                {
                    'itemId': '546a1844ff34c70456111185',
                    'path': ''
                }
            ],
            'commands': [
                ''
            ],
            'name': 'test',
            'output': [{
                'itemId': '546a1844ff34c70456111185'
            }]
        }

        json_body = json.dumps(body)

        r = self.request('/jobs', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        job_id = r.json['_id']

        r = self.request('/clusters/%s/job/%s/submit' % (str(cluster_id), str(job_id)), method='PUT',
                         type='application/json', body='', user=self._user)
        expected_response = {
            u'message': u'Cluster is not running', u'type': u'rest'}
        self.assertEqual(r.json, expected_response)
        self.assertStatus(r, 400)

        # Move cluster into running state
        status_body = {
            'status': 'running'
        }

        r = self.request(
            '/clusters/%s' % str(cluster_id), method='PATCH',
            type='application/json', body=json.dumps(status_body),
            user=self._cumulus)

        self.assertStatusOk(r)

        r = self.request('/clusters/%s/job/%s/submit' % (str(cluster_id), str(job_id)), method='PUT',
                         type='application/json', body='', user=self._user)
        self.assertStatusOk(r)

        expected_submit_call = \
        [   [   [   u'token',
             {   u'_id': cluster_id,
                 u'config': {
                    u'scheduler': {
                        u'type': u'sge'
                    },
                    u'ssh': {
                        u'user': u'ubuntu',
                        u'key': str(self._user_profile['_id'])
                    },
                    u'launch': {
                        u'spec': u'default',
                        u'params': { }
                    }
                },
                 u'name': u'test',
                 u'profileId': str(self._user_profile['_id']),
                 u'status': u'running',
                 u'type': u'ec2',
                 u'userId': str(self._user['_id'])},
             {   u'_id': job_id,
                 u'clusterId': cluster_id,
                 u'commands': [u''],
                 u'input': [   {   u'itemId': u'546a1844ff34c70456111185',
                                   u'path': u''}],
                 u'name': u'test',
                 u'onComplete': {   u'cluster': u'terminate'},
                 u'output': [{   u'itemId': u'546a1844ff34c70456111185'}],
                 u'status': u'created',
                 u'userId': str(self._user['_id'])},
             u'%s/jobs/%s/log' % (cumulus.config.girder.baseUrl, job_id)],
         {   }]]

        self.assertCalls(submit.call_args_list, expected_submit_call)

    @mock.patch('cumulus.ansible.tasks.cluster.terminate_cluster.delay')
    def test_terminate(self, terminate_cluster):

        body = {
            'profileId': str(self._user_profile['_id']),
            'name': 'test'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        # Move cluster into running state
        status_body = {
            'status': 'running'
        }
        r = self.request('/clusters/%s' %
                         str(cluster_id),
                         type='application/json', method='PATCH',
                         body=json.dumps(status_body), user=self._user)

        r = self.request(
            '/clusters/%s/terminate' % str(cluster_id),
            method='PUT', type='application/json',
            body=json.dumps(status_body), user=self._user)

        self.assertStatusOk(r)

        group_id = ModelImporter.model('cluster', 'cumulus').get_group_id()
        expected = \
            [   [   [   u'default',
                 {   u'_id': cluster_id,
                     u'config': {
                        u'scheduler': {
                            u'type': u'sge'
                        },
                        u'ssh': {
                            u'user': u'ubuntu',
                            u'key': str(self._user_profile['_id'])
                        },
                        u'launch': {
                            u'spec': u'default',
                            u'params': {
                                u'cluster_state': u'absent'
                            }
                        }
                     },
                     u'name': u'test',
                     u'profileId': str(self._user_profile['_id']),
                     u'status': u'terminating',
                     u'type': u'ec2',
                     u'userId': str(self._user['_id'])},
                 {   u'_id': str(self._user_profile['_id']),
                     u'accessKeyId': u'id',
                     u'availabilityZone': u'us-west-1a',
                     u'name': u'test',
                     u'publicIPs': False,
                     u'regionHost': u'ec2.us-west-1.amazonaws.com',
                     u'regionName': u'us-west-1',
                     u'status': u'available'},
                 u'secret',
                 {   u'cluster_state': u'absent'},
                 u'token',
                 u'%s/clusters/%s/log' % (cumulus.config.girder.baseUrl, cluster_id),
                 u'terminated'],
             {   }]]

        self.assertCalls(
            terminate_cluster.call_args_list, expected)

    def test_delete(self):
        body = {
            'profileId': str(self._user_profile['_id']),
            'name': 'test'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        r = self.request('/clusters/%s' %
                         str(cluster_id), method='DELETE', user=self._cumulus)
        self.assertStatusOk(r)

        r = self.request('/clusters/%s' %
                         str(cluster_id), method='GET', user=self._cumulus)
        self.assertStatus(r, 404)

        # Test trying to delete a cluster that is running
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = str(r.json['_id'])

        status_body = {
            'status': 'running'
        }
        r = self.request(
            '/clusters/%s' % cluster_id, method='PATCH',
            type='application/json', body=json.dumps(status_body),
            user=self._user)
        self.assertStatusOk(r)

        r = self.request('/clusters/%s' %
                         cluster_id, method='DELETE', user=self._cumulus)
        self.assertStatus(r, 400)



    def test_create_invalid_type(self):
        body = {
            'type': 'bogus'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)
        self.assertEqual(r.json, {
                         u'message': u'Invalid cluster type.', u'type': u'rest'}, 'Unexpected error message')

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_create_trad_type(self, generate_key):
        body = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'ssh': {
                    'user': 'bob'
                },
                'host': 'myhost'
            }
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        cluster_id = r.json['_id']
        expected = [[[{u'status': u'creating', u'userId': str(self._user['_id']), u'type': u'trad', u'_id': cluster_id, u'config': {
            u'host': u'myhost', u'ssh': {u'key': cluster_id, u'user': u'bob'}, u'scheduler': {u'type': u'sge'}}, u'name': u'my trad cluster'}, u'token'], {}]]
        self.assertCalls(
            generate_key.call_args_list, expected)

    @mock.patch('cumulus.tasks.cluster.test_connection.delay')
    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_start_trad(self, generate_key, test_connection):
        body = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'ssh': {
                    'user': 'bob'
                },
                'host': 'myhost'
            }
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        cluster_id = str(r.json['_id'])
        r = self.request('/clusters/%s/start' % cluster_id, method='PUT',
                         user=self._user)

        self.assertStatus(r, 400)
        self.assertEqual(r.json['message'], 'Cluster is not ready to start.',
                         'Unexpected error message')

        # Now update the cluster state to created and try again
        update_body = {
            'status': 'created'
        }

        r = self.request(
            '/clusters/%s' % cluster_id, method='PATCH',
            type='application/json', body=json.dumps(update_body),
            user=self._cumulus)
        self.assertStatusOk(r)
        r = self.request('/clusters/%s/start' % cluster_id, method='PUT',
                         user=self._user)

        self.assertStatusOk(r)
        expected = [[[{u'status': u'created', u'userId': str(self._user['_id']), u'config': {u'host': u'myhost', u'ssh': {u'user': u'bob', u'key': cluster_id}, u'scheduler': {u'type': u'sge'}}, u'_id': cluster_id, u'type': u'trad', u'name': u'my trad cluster'}], {u'girder_token': u'token', u'log_write_url': u'%s/clusters/%s/log' % (cumulus.config.girder.baseUrl, cluster_id)}]]
        self.assertEqual(expected, self.normalize(test_connection.call_args_list))


    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_terminate_trad(self, generate_key):
        body = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'ssh': {
                    'user': 'bob'
                },
                'host': 'myhost'
            }
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)

        self.assertStatus(r, 201)
        _id = str(r.json['_id'])
        r = self.request('/clusters/%s/terminate' % _id, method='PUT',
                         type='application/json', body='', user=self._user)

        self.assertStatus(r, 400)

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_find(self, generate_key):
        # Create a EC2 cluster
        body = {
            'profileId': str(self._user_profile['_id']),
            'name': 'test'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        ec2_cluster_id = r.json['_id']

        # Create a traditional cluster
        body = {
            'config': {
                'ssh': {
                    'user': 'billy'
                },
                'host': 'home'
            },
            'name': 'trad_test',
            'type': 'trad'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        trad_cluster_id = r.json['_id']

        # Search for the EC2 cluster
        params = {
            'type': 'ec2'
        }
        r = self.request(
            '/clusters', method='GET', params=params, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 1, 'Only expecting a single cluster')
        self.assertEqual(r.json[0]['_id'], ec2_cluster_id, 'Returned cluster doesn\'t match')

        # Search for the trad cluster
        params = {
            'type': 'trad'
        }
        r = self.request(
            '/clusters', method='GET', params=params, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 1, 'Only expecting a single cluster')
        expected_cluster = {
            u'status': u'creating',
            u'userId': str(self._user['_id']),
            u'type': u'trad',
            u'_id': trad_cluster_id,
            u'config': {
                u'host': u'home',
                u'ssh': {
                    u'user': u'billy',
                    u'key': trad_cluster_id
                },
                u'scheduler': {
                    u'type': u'sge'
                }
            },
            u'name': u'trad_test'
        }
        self.assertEqual(r.json[0], expected_cluster, 'Returned cluster doesn\'t match')

        # Check limit works
        r = self.request(
            '/clusters', method='GET', params={}, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 2, 'Two clusters expected')

        params = {
            'limit': 1
        }
        r = self.request(
            '/clusters', method='GET', params=params, user=self._user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 1, 'One cluster expected')

        # Checkout that only owner can see clusters
        r = self.request(
            '/clusters', method='GET', params={}, user=self._another_user)
        self.assertStatusOk(r)
        self.assertEqual(len(r.json), 0, 'Don\'t expect any clusters')

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    @mock.patch('cumulus.ssh.tasks.key.delete_key_pair.delay')
    def test_delete_assetstore (self, delete_key, generate_key):
        body = {
            'type': 'trad',
            'name': 'my trad cluster',
            'config': {
                'ssh': {
                    'user': 'bob'
                },
                'host': 'myhost'
            }
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster = r.json
        cluster_id = cluster['_id']

        # Create an assetstore for this cluster
        url_base = get_assetstore_url_base(cluster)
        create_url = '/%s' % url_base
        body = {
            'name': cluster['_id'],
            'host': cluster['config']['host'],
            'user': 'bob',
            'authKey': cluster['_id']
        }
        json_body = json.dumps(body)

        r = self.request(create_url, type='application/json', method='POST',
                         body=json_body, user=self._user)
        self.assertStatusOk(r)
        cluster['assetstoreId'] = r.json['_id']

        # Patch the cluster so it is associated with the assetstore
        patch_cluster = {
            'assetstoreId': str(r.json['_id'])
        }
        r = self.request('/clusters/%s' %
                         str(cluster_id), type='application/json', method='PATCH', body=json.dumps(patch_cluster), user=self._user)
        self.assertStatusOk(r)

        r = self.request('/clusters/%s' %
                         str(cluster_id), method='DELETE', user=self._user)
        self.assertStatusOk(r)

        r = self.request('/clusters/%s' %
                         str(cluster_id), method='GET', user=self._user)
        self.assertStatus(r, 404)

        # Assert that assetstore is gone
        self.assertIsNone(ModelImporter.model('assetstore').load(cluster['assetstoreId']))

    @mock.patch('cumulus.ssh.tasks.key.generate_key_pair.delay')
    def test_create_scheduler_type(self, generate_key_pair):
        trad_body = {
            'config': {
                'host': 'myhost',
                'ssh': {
                    'user': 'myuser'
                },
                'scheduler': {
                    'type': 'bogus'
                }
            },
            'name': 'mycluster',
            'type': 'trad'
        }
        json_body = json.dumps(trad_body)
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 400)

        trad_body['config']['scheduler']['type'] = 'slurm'
        json_body = json.dumps(trad_body)
        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)

    def test_cluster_sse(self):
        body = {
            'profileId': str(self._user_profile['_id']),
            'name': 'test'
        }

        json_body = json.dumps(body)

        r = self.request('/clusters', method='POST',
                         type='application/json', body=json_body, user=self._user)
        self.assertStatus(r, 201)
        cluster_id = r.json['_id']

        # connect to cluster notification stream
        stream_r = self.request('/notification/stream', method='GET', user=self._user,
                         isJson=False, params={'timeout': 0})
        self.assertStatusOk(stream_r)

        # add a log entry
        log_entry = {
            'msg': 'Some message'
        }
        r = self.request('/clusters/%s/log' % str(cluster_id), method='POST',
                         type='application/json', body=json.dumps(log_entry), user=self._user)
        self.assertStatusOk(r)

        notifications = self.getSseMessages(stream_r)

        # we get 2 notifications, 1 from the creation and 1 from the log
        self.assertEqual(len(notifications), 2, 'Expecting two notification, received %d' % len(notifications))
        self.assertEqual(notifications[0]['type'], 'cluster.status', 'Expecting an event with type \'cluster.status\'')
        self.assertEqual(notifications[1]['type'], 'cluster.log', 'Expecting a message with type \'cluster.log\'')
