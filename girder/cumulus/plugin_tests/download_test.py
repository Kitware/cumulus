#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2017 Kitware Inc.
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
import json

from tests import base
import cumulus
from cumulus.transport.files.download import download_path
from girder.constants import AssetstoreType
from girder.utility.module_importer import ModuleImporter

def setUpModule():
    port = 9080
    cherrypy.server.socket_port = port
    cumulus.config.girder.baseUrl = 'http://localhost:%d/api/v1' % port
    base.enabledPlugins.extend(['cumulus', 'sftp'])
    base.startServer(mock=False)


def tearDownModule():
    base.stopServer()

class DownloadTestCase(base.TestCase):
    maxDiff = None
    def setUp(self):
        super(DownloadTestCase, self).setUp()

        self._user = ModelImporter.model('user').createUser(
            email='regularuser@email.com', login='regularuser',
            firstName='First', lastName='Last', password='goodpassword')

        self._folder = six.next(ModelImporter.model('folder').childFolders(
            self._user, parentType='user', force=True, filters={
                'name': 'Public'
            }))

        files_path = os.path.abspath('plugins/cumulus/plugin_tests/fixtures/sftp_paths.json')

        with open(files_path) as fp:
            self.paths = json.load(fp)

        # Create an SFTP assetstore to hold the files
        self._assetstore = ModelImporter.model('assetstore').save({
            'type': AssetstoreType.SFTP,
            'name': 'test',
            'sftp': {
                'host': 'hosty',
                'user': 'billy',
                'authKey': 'super secret'
            }
        })

        self._token = ModelImporter.model('token').createToken(user=self._user)

    def test_download(self):
        cluster_connection = mock.MagicMock()
        cluster_connection.list.side_effect = self.paths

        download_path(cluster_connection, self._token['_id'], self._folder['_id'] , '/test',
                'sftp_assetstores', self._assetstore['_id'], upload=False)

        files = []
        folders = []

        for (p, _) in ModelImporter.model('folder').fileList(self._folder, user=self._user, path='/', data=False):
                files.append(p)

        def traverse_folders(folder, path='/Public'):
            for f in ModelImporter.model('folder').childFolders(folder, 'folder', user=self._user):
                print('trav: path: %s' % (path))
                current_path = os.path.join(path, f['name'])
                print('trav: path: %s' % (path))
                folders.append(current_path)
                traverse_folders(f, path=current_path)

        traverse_folders(self._folder)

        files.sort()
        folders.sort()

        expected_path = os.path.abspath('plugins/cumulus/plugin_tests/fixtures/expected_paths.json')
        with open(expected_path) as fp:
            expected = json.load(fp)

        self.assertEqual(files, expected['files'])
        self.assertEqual(folders, expected['folders'])


