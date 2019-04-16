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
import re
import six

from girder_client import GirderClient

import cumulus
from cumulus.transport import get_connection
from cumulus.transport.files import get_assetstore_url_base, get_assetstore_id


def _include(path, includes, excludes):
    """
    :params path: The file path to check whether to include
    :params includes: List of include regexs to apply
    :params excludes: List of exclude regexs to apply

    :returns True if the file path should be included, False

    """
    if includes is None:
        includes = []

    if excludes is None:
        excludes = []

    # If there are no regexs include the path
    if not includes and not excludes:
        return True

    include = not includes
    for i in includes:
        if re.match(i, path):
            include = True
            break

    for e in excludes:
        if re.match(e, path):
            include = False
            break

    return include


def _ensure_path(girder_client, girder_folders, parent, path):
    """
    Ensure that a particular path exists in Girder as a set of folders. If
    it doesn't exist create it.

    :params girder_client: The Girder client to use to access Girder.
    :params girder_folders: A map of paths to existing Girder folder ids.
    :params parent: The parent Girder folder that this path should exist in.
    :params path: The path that should exist.

    :returns The folderId for the last folder in the path.
    """
    if path in girder_folders:
        return girder_folders[path]

    parts = path.split('/')

    # Default our parent_id to parent, however this may be updated if we already
    # have folders created in Girder, see loop below.
    parent_id = parent

    # First identify the folders we have already have in Girder
    index = 0
    for i in range(len(parts), 0, -1):
        part = '/'.join(parts[:i])
        if part in girder_folders:
            parent_id = girder_folders[part]
            index = i
            break

    # Now walk through the parts of the path we need to create
    i = index + 1
    parent_created = False
    for name in parts[index:]:
        path = '/'.join(parts[:i])
        # Check if the folder already exists
        if not parent_created:
            folders = girder_client.listFolder(parent_id, name=name)
            try:
                folder_id = six.next(folders)['_id']
                girder_folders[path] = folder_id
                parent_id = folder_id
                i += 1
                continue
            except StopIteration:
                pass

        # Create the folder
        folder = girder_client.createFolder(parent_id, name,
                                            parentType='folder')
        # Don't bother checking if folders have already been created ...
        parent_created = True
        parent_id = folder['_id']
        girder_folders[path] = parent_id

        i += 1

    return parent_id


def _import_path(cluster_connection, girder_client, parent, root_path,
                 assetstore_url, assetstore_id, upload=False,
                 include=None, exclude=None, path='.', girder_folders=None):
    """
    :params cluster_connection: The cluster connection to access the cluster.
    :params girder_client: The Girder client to use to access Girder.
    :params parent: The target folder to import the path into.
    :params root_path: The path on the cluster being imported.
    :params assetstore_url: The url for the assetstore to use for the import.
    :params assetstore_id: The id of the asseststore to import into.
    :params upload: Indicate if the import should upload the file data or just
                    the metadata, the default is False.
    :params include: List of include regexs
    :params exclude: List of exclude regexs,
    :params path: The current subdirectory of root_path that is being imported.
    """
    if girder_folders is None:
        girder_folders = {}

    if root_path[0] != '/':
        # If we don't have a full path, assume the path is relative to the users
        # home directory.
        home = cluster_connection.execute('pwd')[0]
        root_path = os.path.abspath(os.path.join(home, root_path))

    cluster_path = os.path.normpath(os.path.join(root_path, path))
    for p in cluster_connection.list(cluster_path):
        name = p['name']

        full_path = os.path.normpath(os.path.join(path, name))
        if name in ['.', '..']:
            continue

        if stat.S_ISDIR(p['mode']):
            _import_path(cluster_connection, girder_client, parent,
                         root_path, assetstore_url, assetstore_id,
                         upload=upload, path=full_path,
                         include=include,
                         exclude=exclude, girder_folders=girder_folders)
        else:

            # Should we include this path?
            if not _include(full_path, include, exclude):
                continue

            # Create any folders we might need
            if path == '.':
                folder_id = parent
            else:
                folder_id = _ensure_path(girder_client, girder_folders, parent,
                                         path)

            size = p['size']
            item = girder_client.createItem(folder_id, name, '')

            if not upload:

                url = '%s/%s/files' % (assetstore_url, assetstore_id)
                body = {
                    'name': name,
                    'itemId': item['_id'],
                    'size': size,
                    'path': os.path.join(root_path, full_path)
                }
                girder_client.post(url, data=json.dumps(body))
            else:
                cluster_path = os.path.normpath(
                    os.path.join(root_path, full_path))
                with cluster_connection.get(cluster_path) as stream:
                    girder_client.uploadFile(item['_id'], stream, name, size,
                                             parentType='item')


def download_path(cluster_connection, girder_token, parent, path,
                  assetstore_url, assetstore_id, upload=False, include=None,
                  exclude=None):
    """
    Download a given path on a cluster into an assetstore.

    :params cluster_connection: The cluster connection to access the cluster.
    :params girder_token: The Girder token to use to access Girder.
    :params parent: The target folder to import the path into.
    :params path: The path on the cluster to download.
    :params assetstore_url: The url for the assetstore to use for the import.
    :params assetstore_id: The id of the asseststore to import into.
    :params upload: Indicate if the import should upload the file data or just
                    the metadata, the default is False.
    :params include: List of include regexs
    :params exclude: List of exclude regexs,
    """
    girder_client = GirderClient(apiUrl=cumulus.config.girder.baseUrl)
    girder_client.token = girder_token

    _import_path(cluster_connection, girder_client, parent, path,
                 assetstore_url, assetstore_id, upload=upload, include=include,
                 exclude=exclude)


def download_path_from_cluster(cluster, girder_token, parent, path,
                               upload=False, include=None, exclude=None):
    """
    Download a given path on a cluster into an assetstore.

    :params cluster: The cluster to to download the path from.
    :params girder_token: The Girder token to use to access Girder.
    :params parent: The target folder to import the path into.
    :params path: The path on the cluster to download.
    :params upload: Indicate if the import should upload the file data or just
                    the metadata, the default is False.
    :params include: List of include regexs
    :params exclude: List of exclude regexs,
    """
    assetstore_base_url = get_assetstore_url_base(cluster)
    assetstore_id = get_assetstore_id(girder_token, cluster)

    with get_connection(girder_token, cluster) as conn:
        download_path(conn, girder_token, parent, path, assetstore_base_url,
                      assetstore_id, upload=upload, include=include,
                      exclude=exclude)
