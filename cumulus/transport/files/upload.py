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

from girder_client import GirderClient
import requests

import cumulus
from cumulus.common import check_status
from cumulus.transport import get_connection


def _upload_file(cluster_connection, girder_client, file, path):
    """
    Upload a file to a cluster

    :param cluster_connection: The connection to access the cluster by.
    :param girder_client: The Grider client for Girder access.
    :param file: The Girder file object.
    :param path: The path on the cluster to upload to.
    """

    r = requests.get(
        '%s/file/%s/download' % (girder_client.urlBase, file['_id']),
        headers={'Girder-Token': girder_client.token}, stream=True)
    check_status(r)
    cluster_connection.put(r.raw, os.path.join(path, file['name']))


def _upload_item(cluster_connection, girder_client, item, path):
    offset = 0
    params = {
        'limit': 50,
        'offset': offset
    }

    while True:
        files = girder_client.get('item/%s/files' % item['_id'],
                                  parameters=params)

        for file in files:
            _upload_file(cluster_connection, girder_client, file, path)

        offset += len(files)
        if len(files) < 50:
            break


def _upload_items(cluster_connection, girder_client, folder_id, path):
    for item in girder_client.listItem(folder_id):
        _upload_item(cluster_connection, girder_client, item, path)


def _upload_path(cluster_connection, girder_client, folder_id, path):
    # First process items
    _upload_items(cluster_connection, girder_client, folder_id, path)

    # Now folders
    for folder in girder_client.listFolder(folder_id):
        folder_path = os.path.join(path, folder['name'])
        cluster_connection.mkdir(folder_path)
        _upload_path(cluster_connection, girder_client, folder['_id'],
                     folder_path)


def upload_path(cluster_connection, girder_token, folder_id, path):
    girder_client = GirderClient(apiUrl=cumulus.config.girder.baseUrl)
    girder_client.token = girder_token
    cluster_connection.makedirs(path)

    _upload_path(cluster_connection, girder_client, folder_id, path)


def upload_file(cluster, girder_token, file, path):
    """
    Upload a file to a cluster

    :param cluster: The cluster to upload to.
    :param girder_tokebn: The Grider token for Girder access.
    :param file: The Girder file object.
    :param path: The path on the cluster to upload to.
    """
    girder_client = GirderClient(apiUrl=cumulus.config.girder.baseUrl)
    girder_client.token = girder_token
    with get_connection(girder_token, cluster) as conn:
        conn.makedirs(os.path.dirname(path))
        _upload_file(conn, girder_client, file, path)
