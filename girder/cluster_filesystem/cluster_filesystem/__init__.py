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
from girder.constants import AssetstoreType, AccessType, TokenScope
from girder.utility.assetstore_utilities import setAssetstoreAdapter
from girder.models.model_base import ValidationException
from girder.plugin import getPlugin, GirderPlugin
from girder.utility import setting_utilities
from girder.api.describe import Description, autoDescribeRoute

from cumulus.transport import get_connection
from cumulus_plugin.models.cluster import Cluster
from girder.api.rest import getCurrentUser, getCurrentToken, RestException

import datetime as dt

def date_parser(timestring):
    """
        Parse a datetime string from ls -l and return a standard isotime

        ls -l returns dates in two different formats:

        May  2 09:06 (for dates within 6 months from now)
        May  2 2018  (for dates further away)

        Best would be to return ls -l --full-time,
        but unfortunately we have no control over the remote API
    """

    recent_time_format = "%b %d %H:%M"
    older_time_format = "%b %d %Y"
    try:
        date = dt.datetime.strptime(timestring, recent_time_format)
        now = dt.datetime.now()
        this_year = dt.datetime(year=now.year,
                           month=date.month, day=date.day,
                           hour=date.hour, minute=date.minute)
        last_year = dt.datetime(year=now.year-1,
                           month=date.month, day=date.day,
                           hour=date.hour, minute=date.minute)

        delta_this = abs((now-this_year).total_seconds())
        delta_last = abs((now-last_year).total_seconds())
        if (delta_this > delta_last):
            date = last_year
        else:
            date = this_year

    except ValueError:
        try:
            date = dt.datetime.strptime(timestring, older_time_format)
        except ValueError:
            return timestring
    return date.isoformat()

def _mtime_isoformat(mtime):
    return dt.datetime.fromtimestamp(mtime).isoformat()


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

        cluster_id = None
        try:
            decoded_id = urllib.parse.unquote_plus(id)
            (cluster_id, path) = _parse_id(decoded_id)
            # If we have successfully decoded the id, then prevent the default
            event.preventDefault()

        except ValueError:
            pass

        if cluster_id is not None:
            cluster = Cluster().load(cluster_id,  user=getCurrentUser())

            token = getCurrentToken()
            with get_connection(token['_id'], cluster) as conn:
               response = func(conn, path, cluster=cluster, encoded_id=id)

            event.addResponse(response)

    return wrapped


@access.user(scope=TokenScope.DATA_READ)
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
                'created': date_parser(entry['date']),
                'description': '',
                'name': entry['name'],
                'parentCollection': 'folder',
                'parentId': encoded_id,
                'public': False,
                'size': entry['size'],
                'updated': date_parser(entry['date'])
            })

    return folders

@access.user(scope=TokenScope.DATA_READ)
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
                'created': date_parser(entry['date']),
                'description': '',
                'name': name,
                'parentCollection': 'folder',
                'parentId': parent_id,
                'public': False,
                'size': entry['size'],
                'updated': date_parser(entry['date'])
            }


@access.user(scope=TokenScope.DATA_READ)
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
                "created": date_parser(entry['date']),
                "description": "",
                "folderId": encoded_id,
                "name": entry['name'],
                "size": entry['size'],
                "updated": date_parser(entry['date']),
            })

    return items

@access.user(scope=TokenScope.DATA_READ)
@_decode_id
def _item_id_before(conn, path, cluster, encoded_id):
    file_stat = conn.stat(path)

    parent_path = os.path.dirname(path)
    name = os.path.basename(path)
    parent_id = _generate_id(cluster['_id'], parent_path)

    return {
        "_id": encoded_id,
        "_modelType": "item",
        "created": _mtime_isoformat(file_stat.st_mtime),
        "description": "",
        "folderId": parent_id,
        "name": name,
        "size": file_stat.st_size,
        "updated": _mtime_isoformat(file_stat.st_mtime)
    }


@access.user(scope=TokenScope.DATA_READ)
@_decode_id
def _item_files_before(conn, path, cluster, encoded_id):
    file_stat = conn.stat(path)
    name = os.path.basename(path)
    return {
        "_id": encoded_id,
        "_modelType": "file",
        "assetstoreId": None,
        "created": _mtime_isoformat(file_stat.st_mtime),
        "exts": [
            os.path.splitext(name)[1]
        ],
        "itemId": encoded_id,
        "mimeType": "application/octet-stream",
        "name": name,
        "size": file_stat.st_size,
        "updated": _mtime_isoformat(file_stat.st_mtime)
    }


@access.user(scope=TokenScope.DATA_READ)
def _file_id_before(event):
    return _item_files_before(event)


@access.public(cookie=True)
@_decode_id
def _file_download_before(conn, path, cluster_id, **rest):
    return conn.get(path)

# Rest endpoint to start file system traversal.
@access.user(scope=TokenScope.DATA_READ)
@autoDescribeRoute(
    Description('Fetches information about a path on the clusters filesystem.')
    .modelParam('id', 'The cluster id',
                model=Cluster, destName='cluster',
                level=AccessType.READ, paramType='path')
    .param('path', 'The filesystem path.', required=True, paramType='query')
)


def _get_path(cluster, path):
    basename = os.path.basename(path)
    token = getCurrentToken()

    with get_connection(token['_id'], cluster) as conn:
        entry = conn.stat(path)

        entry_id = _generate_id(cluster['_id'], path)
        parent_id = _generate_id(cluster['_id'], os.path.dirname(path))
        model = {
            '_id': entry_id,
            'size': entry.st_size,
            'name': basename,
            'created': _mtime_isoformat(entry.st_mtime),
            'updated': _mtime_isoformat(entry.st_mtime)
        }
        if stat.S_ISDIR(entry.st_mode):
            model['_modelType'] =  'folder'
            model['description'] =  ''
            model['parentCollection'] = 'folder'
            model['parentId'] =  parent_id
            model['public'] =  False

            return model
        elif stat.S_ISREG(entry.st_mode):
            model['_modelType'] = "file"
            model['assetstoreId'] = None
            model["exts"] = [
                os.path.splitext(basename)[1]
            ]
            model['itemId'] = parent_id,
            model['mimeType'] = 'application/octet-stream'

            return model

class ClusterFileSystemPlugin(GirderPlugin):
    DISPLAY_NAME = 'Cluster filesystem'

    def load(self, info):
        getPlugin('cumulus_plugin').load(info)

        events.bind('rest.get.folder.before', 'cluster_filesystem',_folder_before)
        events.bind('rest.get.folder/:id.before', 'cluster_filesystem',_folder_id_before)
        events.bind('rest.get.item.before', 'cluster_filesystem',_item_before)
        events.bind('rest.get.item/:id.before', 'cluster_filesystem',_item_id_before)
        events.bind('rest.get.item/:id/files.before', 'cluster_filesystem', _item_files_before)
        events.bind('rest.get.file/:id.before', 'cluster_filesystem',_file_id_before)
        events.bind('rest.get.file/:id/download.before', 'cluster_filesystem',_file_download_before)

        info['apiRoot'].clusters.route('GET', (':id', 'filesystem'), _get_path)
