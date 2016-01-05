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

import os
import stat
import json

from girder_client import GirderClient

import cumulus


def _import_path(cluster_connection, girder_client, parent, path,
                 assetstore_url, assetstore_id, upload=False,
                 parent_type='folder'):

    if path[0] != '/':
        # If we don't have a full path, assume the path is relative to the users
        # home directory.
        home = cluster_connection.execute('pwd')[0]
        path = os.path.abspath(os.path.join(home, path))

    for p in cluster_connection.list(path):
        name = p['name']

        full_path = os.path.join(path, name)
        if name in ['.', '..']:
            continue
        if stat.S_ISDIR(p['mode']):

            folder = girder_client.createFolder(parent, name,
                                                parentType=parent_type)
            _import_path(cluster_connection, girder_client, folder['_id'],
                         full_path, assetstore_url, assetstore_id,
                         upload=upload, parent_type='folder')
        else:
            size = p['size']
            item = girder_client.createItem(parent, name, '')

            if not upload:

                url = '%s/%s/files' % (assetstore_url, assetstore_id)
                body = {
                    'name': name,
                    'itemId': item['_id'],
                    'size': size,
                    'path': full_path
                }
                girder_client.post(url, data=json.dumps(body))
            else:
                with girder_client.get(path) as stream:
                    girder_client.uploadFile(item['_id'], stream, name, size,
                                             parentType='item')


def download_path(cluster_connection, girder_token, parent, path,
                  assetstore_url, assetstore_id, upload=False):
    girder_client = GirderClient(apiUrl=cumulus.config.girder.baseUrl)
    girder_client.token = girder_token

    _import_path(cluster_connection, girder_client, parent, path,
                 assetstore_url, assetstore_id, upload=upload)
