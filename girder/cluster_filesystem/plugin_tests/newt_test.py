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

from .constants import (
    CLUSTER_LOAD, PATH, ID_FIELDS,
    CURR_DIR, PARENT_DIR,
    DIR1, DIR2, DIR3,
    FILE1, FILE2, FILE3
)


def _assert_dir(listing, received, id_fields, is_curr=False):
    if is_curr:
        dir_name = os.path.basename(id_fields['path'])
        assert dir_name  == received['name']
    else:
        assert listing['name'] == received['name']
    assert listing['size'] == received['size']
    assert not received['public']
    assert received['parentCollection'] == 'folder'
    
    listed_parent = id_fields.copy()
    received_parent = json.loads(urllib.parse.unquote_plus(received['parentId']))
    _assert_parent(listed_parent, received_parent, is_curr)
    
    id = json.loads(urllib.parse.unquote_plus(received['_id']))
    assert id['clusterId'] == 'dummy'
    #assert id['path'] == os.path.join(parent_id['path'], received['name'])
    if listing['mode'] == stat.S_IFDIR:
        assert received['_modelType'] == 'folder'
    elif listing['mode'] == stat.S_IFREG:
        assert received['_modelType'] == 'file'

def _assert_base():
    pass

def _assert_parent(listed, received, is_curr=False):
    if is_curr:
        listed['path'] = os.path.dirname(listed['path'])
    assert listed == received
    

def _set_mock_connection_list(conn):
    conn.list.return_value = [PARENT_DIR, CURR_DIR]
    folders = [DIR1, DIR2, DIR3]
    files  = [FILE1, FILE2, FILE3]
    conn.list.return_value.extend(folders)
    conn.list.return_value.extend(files)
    return folders, files

def _set_mock_cluster(cluster):
    cluster_model = cluster.return_value
    cluster_model.load.return_value = CLUSTER_LOAD
    return
    


@pytest.mark.plugin('cluster_filesystem')
@mock.patch('girder.plugins.cluster_filesystem.get_connection')
@mock.patch('girder.plugins.cluster_filesystem.Cluster')
def test_folder(cluster, get_connection, server, user):
    conn = get_connection.return_value.__enter__.return_value
    folders, files = _set_mock_connection_list(conn)
    _set_mock_cluster(cluster)

    id = urllib.parse.quote_plus(json.dumps(ID_FIELDS))

    params = {
        'parentId': id
    }
    r = server.request('/folder', method='GET',
                       type='application/json', params=params, user=user)
    assertStatusOk(r)

    received_folders = r.json
    assert len(received_folders) == len(folders)
    for listed, received in zip(folders, received_folders):
        _assert_dir(listed, received, ID_FIELDS)

    assert conn.list.call_args_list == [mock.call(PATH)]

@pytest.mark.plugin('cluster_filesystem')
@mock.patch('girder.plugins.cluster_filesystem.get_connection')
@mock.patch('girder.plugins.cluster_filesystem.Cluster')
def test_folder_id(cluster, get_connection, server, user):
    conn = get_connection.return_value.__enter__.return_value
    conn.list.return_value = [
        CURR_DIR
    ]

    _set_mock_cluster(cluster)

    id = urllib.parse.quote_plus(json.dumps(ID_FIELDS))

    r = server.request('/folder/%s' % id, method='GET',
                       type='application/json', user=user)
    assertStatusOk(r)
    folder = r.json
    _assert_dir(CURR_DIR, folder, ID_FIELDS, is_curr=True)


@pytest.mark.plugin('cluster_filesystem')
@mock.patch('girder.plugins.cluster_filesystem.get_connection')
@mock.patch('girder.plugins.cluster_filesystem.Cluster')
def test_item(cluster, get_connection, server, user):
    pass


@pytest.mark.plugin('cluster_filesystem')
@mock.patch('girder.plugins.cluster_filesystem.get_connection')
@mock.patch('girder.plugins.cluster_filesystem.Cluster')
def test_item_id(cluster, get_connection, server, user):
    pass