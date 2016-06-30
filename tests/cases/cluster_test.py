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

from cumulus.testing import AssertCallsMixin

class ClusterTestCase(AssertCallsMixin, unittest.TestCase):

    def setUp(self):
        self._get_status_called  = False
        self._set_status_called  = False

    def test_start_cluster_max_instance_limit(self):
        pass
# TODO refactor or remove
#         def valid(self):
#             return True
#
#         starcluster.cluster.ClusterValidator.validate_required_settings = valid
#
#         cluster_id = 'dummy_id'
#         cluster_model = {
#             '_id': cluster_id,
#             'config': {
#                 '_id': 'dummy_config_id'
#             },
#             'name': 'dummy_cluster_name',
#             'template': 'dummy_template'
#         }
#
#         def _get_status(url, request):
#             content = {
#                 'status': 'initializing'
#             }
#             content = json.dumps(content)
#             headers = {
#                 'content-length': len(content),
#                 'content-type': 'application/json'
#             }
#
#             self._get_status_called  = True
#             return httmock.response(200, content, headers, request=request)
#
#         self._set_call_value_index = 0
#         def _set_status(url, request):
#             expected = [{'status': 'initializing'}, {'status': 'error'}]
#             self._set_status_called = json.loads(request.body) == expected[self._set_call_value_index]
#             self.assertEqual(json.loads(request.body),
#                              expected[self._set_call_value_index],
#                               'Unexpected status update body')
#             self._set_call_value_index += 1
#             return httmock.response(200, None, {}, request=request)
#
#         status_url = '/api/v1/clusters/%s/status' % cluster_id
#         get_status = httmock.urlmatch(
#             path=r'^%s$' % status_url, method='GET')(_get_status)
#
#         status_update_url = '/api/v1/clusters/%s' % cluster_id
#         set_status = httmock.urlmatch(
#             path=r'^%s$' % status_update_url, method='PATCH')(_set_status)
#
#         with httmock.HTTMock(get_status, set_status):
#             cluster.start_cluster(cluster_model, **{'girder_token': 's', 'log_write_url': 1})
