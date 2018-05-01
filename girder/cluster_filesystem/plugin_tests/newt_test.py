#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2018 Kitware Inc.
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
import json
import mock
import stat
from six.moves import urllib
import pytest
import os

from pytest_girder.assertions import assertStatusOk, assertStatus

from .constants import CURR_DIR, PARENT_DIR, DIR1, DIR2, FILE1


def _assert(listing, received, id_fields, is_curr_dir=False):
    if is_curr_dir:
        dir_name = os.path.split(id_fields['path'])[1]
        assert dir_name  == received['name']
    else:
        assert listing['name'] == received['name']
    assert listing['size'] == received['size']
    assert not received['public']
    assert received['parentCollection'] == 'folder'
    parent_id = json.loads(urllib.parse.unquote_plus(received['parentId']))
    if is_curr_dir:
        pass
    else:
        assert  parent_id == id_fields
    id = json.loads(urllib.parse.unquote_plus(received['_id']))
    assert id['clusterId'] == 'dummy'
    assert id['path'] == os.path.join(parent_id['path'], received['name'])
    if listing['mode'] == stat.S_IFDIR:
        assert received['_modelType'] == 'folder'
    elif listing['mode'] == stat.S_IFREG:
        assert received['_modelType'] == 'file'


@pytest.mark.plugin('cluster_filesystem')
@mock.patch('girder.plugins.cluster_filesystem.get_connection')
@mock.patch('girder.plugins.cluster_filesystem.Cluster')
def test_folder(cluster, get_connection, server, user):
    conn = get_connection.return_value.__enter__.return_value

    conn.list.return_value = [
        PARENT_DIR,
        CURR_DIR,
        DIR1,
        DIR2, {
        'name': 'file1',
        'group': 'group',
        'user': 'user',
        'mode': 0,
        'date': 'Feb  7 16:26',
        'size': 0
    }]
    cluster_model = cluster.return_value
    cluster_model.load.return_value = {
        '_id': 'dummy',
        'type': 'newt',
        'name': 'eyeofnewt',
        'config': {
            'host': 'cori'
        }
    }
    path ='/a/b/c'
    id_fields = {
        'clusterId': 'dummy',
        'path': path
    }
    id = urllib.parse.quote_plus(json.dumps(id_fields))

    params = {
        'parentId': id
    }
    r = server.request('/folder', method='GET',
                       type='application/json', params=params, user=user)
    assertStatusOk(r)
    assert len(r.json) == 2
    (received_dir1, received_dir2) = r.json

    _assert(DIR1, received_dir1, id_fields)
    _assert(DIR2, received_dir2, id_fields)

    assert conn.list.call_args_list == [mock.call(path)]

@pytest.mark.plugin('cluster_filesystem')
@mock.patch('girder.plugins.cluster_filesystem.get_connection')
@mock.patch('girder.plugins.cluster_filesystem.Cluster')
def test_folder_id(cluster, get_connection, server, user):
    conn = get_connection.return_value.__enter__.return_value
    dir = {
        'name': '.',
        'group': 'group',
        'user': 'user',
        'mode': stat.S_IFDIR,
        'date': 'Feb  7 16:26',
        'size': 0
    }
    conn.list.return_value = [
        dir
    ]

    cluster_model = cluster.return_value
    cluster_model.load.return_value = {
        '_id': 'dummy',
        'type': 'newt',
        'name': 'eyeofnewt',
        'config': {
            'host': 'cori'
        }
    }

    path = '/a/b/c'
    id_fields = {
        'clusterId': 'dummy',
        'path': path
    }
    id = urllib.parse.quote_plus(json.dumps(id_fields))

    r = server.request('/folder/%s' % id, method='GET',
                       type='application/json', user=user)
    assertStatusOk(r)
    folder = r.json
    _assert(CURR_DIR, folder, id_fields, is_curr_dir=True)

