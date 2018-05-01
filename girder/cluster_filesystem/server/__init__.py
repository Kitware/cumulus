#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2018 Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the 'License' );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an 'AS IS' BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################

from base64 import b64decode, b64encode
import stat
import os
import json
from six.moves import urllib
import cherrypy
import functools

from girder import events
from girder.api import access
from girder.api.v1.assetstore import Assetstore
from girder.constants import AssetstoreType, AccessType
from girder.utility.assetstore_utilities import setAssetstoreAdapter
from girder.models.model_base import ValidationException
from girder.utility import setting_utilities

from cumulus.transport import get_connection
from girder.plugins.cumulus.models.cluster import Cluster
from girder.api.rest import getCurrentUser, getCurrentToken

NEWT_BASE_URL = 'https://newt.nersc.gov/newt'

def _parse_id(id):
    id = json.loads(id)

    return (id['clusterId'], id['path'])


def _generate_id(cluster_id, path):
    return urllib.parse.quote_plus(json.dumps({
        'clusterId': str(cluster_id),
        'path': path
    }))


def _decode_id(func=None, key='id'):

    if func is None:
        return functools.partial(_decode_id, key=key)

    @functools.wraps(func)
    def wrapped(event, **kwargs):
        if 'params' in event.info and key in event.info['params']:
            id = event.info['params'][key]
        elif key in event.info:
            id = event.info[key]
        else:
            # Request is not well formed, delegate to core.
            return

        try:
            decoded_id = urllib.parse.unquote_plus(id)
            (cluster_id, path) = _parse_id(decoded_id)
            # If we have successfully decoded the id, then prevent the default
            event.preventDefault()

            cluster = Cluster().load(cluster_id,  user=getCurrentUser())

            token = getCurrentToken()
            with get_connection(token['_id'], cluster) as conn:
                response = func(conn, path, cluster=cluster, encoded_id=id)

            event.addResponse(response)
        except ValueError:
            pass


    return wrapped


@access.user
@_decode_id(key='parentId')
def _folder_before(conn, path, cluster, encoded_id, **rest):
    folders = []
    for entry in conn.list(path):
        if stat.S_ISDIR(entry['mode']) and entry['name'] not in ['.', '..']:
            entry_path = os.path.join(path, entry['name'])
            entry_id = _generate_id(cluster['_id'], entry_path)
            folders.append({
                '_id': entry_id,
                '_modelType': 'folder',
                # TODO: Need to convert to right format
                'created': entry['date'],
                'description': '',
                'name': entry['name'],
                'parentCollection': 'folder',
                'parentId': encoded_id,
                'public': False,
                'size': entry['size'],
                # TODO: Need to convert to right format
                'updated': entry['date']
            })

    return folders

@access.user
@_decode_id
def _folder_id_before(conn, path, cluster, encoded_id):
    for entry in conn.list(path):
        if entry['name'] == '.':
            parent_path = os.path.dirname(path)
            name = os.path.basename(path)
            parent_id = _generate_id(cluster['_id'], parent_path)
            return {
                '_id': encoded_id,
                '_modelType': 'folder',
                # TODO: Need to convert to right format
                'created': entry['date'],
                'description': '',
                'name': name,
                'parentCollection': 'folder',
                'parentId': parent_id,
                'public': False,
                'size': entry['size'],
                # TODO: Need to convert to right format
                'updated': entry['date']
            }


@access.user
@_decode_id(key='folderId')
def _item_before(conn, path, cluster, encoded_id):
    items = []
    for entry in conn.list(path):
        if not stat.S_ISDIR(entry['mode']):
            item_path = os.path.join(path, entry['name'])
            item_id = _generate_id(cluster['_id'], item_path)
            items.append({
                "_id": item_id,
                "_modelType": "item",
                # TODO: Need to convert to right format
                "created": entry['date'],
                "description": "",
                "folderId": encoded_id,
                "name": entry['name'],
                "size": entry['size'],
                # TODO: Need to convert to right format
                "updated": entry['date']
            })

    return items

@access.user
@_decode_id
def _item_id_before(conn, path, cluster, encoded_id):
    item = next(conn.list(path))
    
    parent_path = os.path.dirname(path)
    parent_id = _generate_id(cluster['_id'], parent_path)

    return {
        "_id": encoded_id,
        "_modelType": "item",
        # TODO: Need to convert to right format
        "created": item['date'],
        "description": "",
        "folderId": parent_id,
        "name": item['name'],
        "size": item['size'],
        # TODO: Need to convert to right format
        "updated": item['date']
    }


@access.user
@_decode_id
def _item_files_before(conn, path, cluster, encoded_id):
    file = next(conn.list(path))

    return {
        "_id": encoded_id,
        "_modelType": "file",
        "assetstoreId": None,
        # TODO: Need to convert to right format
        "created": file['date'],
        "exts": [
            os.path.splitext(file['name'])[1]
        ],
        "itemId": encoded_id,
        "mimeType": "application/octet-stream",
        "name": file['name'],
        "size": file['size'],
        # TODO: Need to convert to right format
        "updated": file['date'],
    }


@access.user
def _file_id_before(event):
    return _item_files_before(event)


@access.cookie
@_decode_id
def _file_download_before(conn, path, cluster_id, **rest):
    cluster = Cluster().load()
    url = '%s/file/%s/%s?view=read' % (NEWT_BASE_URL, cluster['hostName'], path)
    raise cherrypy.HTTPRedirect(url)


def load(info):
    events.bind('rest.get.folder.before', 'newt_folders',_folder_before)
    events.bind('rest.get.folder/:id.before', 'newt_folders',_folder_id_before)
    events.bind('rest.get.item.before', 'newt_folders',_item_before)
    events.bind('rest.get.item/:id.before', 'newt_folders',_item_id_before)
    events.bind('rest.get.item/:id/files.before', 'new_folders', _item_files_before)
    events.bind('rest.get.file/:id.before', 'newt_folders',_file_id_before)
    events.bind('rest.get.file/:id/download.before', 'newt_folders',_file_download_before)

