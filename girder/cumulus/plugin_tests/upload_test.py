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

import cherrypy
import mock
import os
import six

from tests import base
import cumulus
from cumulus.transport.files.upload import upload_path

from girder.utility.model_importer import ModelImporter

def setUpModule():
    base.enabledPlugins.append('cumulus')
    port = 9080
    cherrypy.server.socket_port = port
    cumulus.config.girder.baseUrl = 'http://localhost:%d/api/v1' % port
    base.startServer(mock=False)


def tearDownModule():
    base.stopServer()


class UploadTestCase(base.TestCase):

    def setUp(self):
        super(UploadTestCase, self).setUp()

        self._user = ModelImporter.model('user').createUser(
            email='regularuser@email.com', login='regularuser',
            firstName='First', lastName='Last', password='goodpassword')

        self._folder = six.next(ModelImporter.model('folder').childFolders(
            self._user, parentType='user', force=True, filters={
                'name': 'Public'
            }))

        item = ModelImporter.model('item').createItem(
            name='bob.txt', creator=self._user, folder=self._folder)

        path = os.path.abspath('plugins/cumulus/plugin_tests/fixtures/bob.txt')
        file = ModelImporter.model('file').createFile(creator=self._user,
                                             item=item, name='bob.txt',
                                             size=os.path.getsize(path),
                                             mimeType='text/plain',
                                             assetstore=self.assetstore)
        file['imported'] = True
        file['path'] = path
        ModelImporter.model('file').save(file)

        path = os.path.abspath('plugins/cumulus/plugin_tests/fixtures/bill.txt')
        file = ModelImporter.model('file').createFile(creator=self._user,
                                             item=item, name='bill.txt',
                                             size=os.path.getsize(path),
                                             mimeType='text/plain',
                                             assetstore=self.assetstore)
        file['imported'] = True
        file['path'] = path
        ModelImporter.model('file').save(file)

        self._sub_folder = ModelImporter.model('folder').createFolder(
            parentType='folder', parent=self._folder, creator=self._user,
            public=False, name='subfolder')


        item = ModelImporter.model('item').createItem(
            name='will.txt', creator=self._user, folder=self._sub_folder)
        path = os.path.abspath('plugins/cumulus/plugin_tests/fixtures/will.txt')
        file = ModelImporter.model('file').createFile(creator=self._user,
                                             item=item, name='will.txt',
                                             size=os.path.getsize(path),
                                             mimeType='text/plain',
                                             assetstore=self.assetstore)
        file['imported'] = True
        file['path'] = path
        ModelImporter.model('file').save(file)

    def test_upload(self):
        cluster_connection = mock.MagicMock()
        token = ModelImporter.model('token').createToken(self._user)
        upload_path(cluster_connection, str(token['_id']),
                    self._folder['_id'], '/tmp')

        cluster_connection.mkdir.assert_has_calls(
            [mock.call(u'/tmp/subfolder')])

        self.assertEqual(len(cluster_connection.put.call_args_list), 3)
        _, (request, path), _ = cluster_connection.put.mock_calls[0]
        self.assertEqual(path, '/tmp/bill.txt')
        self.assertEqual(request.read().decode('utf8').strip(), 'bill')

        _, (request, path), _ = cluster_connection.put.mock_calls[1]
        self.assertEqual(path, '/tmp/bob.txt')
        self.assertEqual(request.read().decode('utf8').strip(), 'bob')

        _, (request, path), _ = cluster_connection.put.mock_calls[2]
        self.assertEqual(path, '/tmp/subfolder/will.txt')
        self.assertEqual(request.read().decode('utf8').strip(), 'will')

