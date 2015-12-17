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

from .assetstore import NewtAssetstoreAdapter
from girder import events
from girder.api import access
from girder.api.v1.assetstore import Assetstore
from girder.constants import AssetstoreType
from girder.utility.model_importer import ModelImporter
from .rest import Newt


def getAssetstore(event):
    assetstore = event.info
    if assetstore['type'] == AssetstoreType.NEWT:
        event.stopPropagation()
        event.addResponse(NewtAssetstoreAdapter)


def updateAssetstore(event):
    params = event.info['params']
    assetstore = event.info['assetstore']

    if assetstore['type'] == AssetstoreType.NEWT:
        assetstore['newt'] = {
            'machine': params.get('machine', assetstore['newt']['machine']),
            'baseUrl': params.get('baseUrl', assetstore['newt']['baseUrl'])
        }


@access.admin
def createAssetstore(event):
    params = event.info['params']

    if params.get('type') == AssetstoreType.NEWT:
        event.addResponse(ModelImporter.model('assetstore').save({
            'type': AssetstoreType.NEWT,
            'name': params.get('name'),
            'newt': {
                'machine': params.get('machine'),
                'baseUrl': params.get('baseUrl')
            }
        }))
        event.preventDefault()


def load(info):

    AssetstoreType.NEWT = 'newt'
    events.bind('assetstore.adapter.get', 'newt_assetstore', getAssetstore)
    events.bind('assetstore.update', 'newt_assetstore', updateAssetstore)
    events.bind('rest.post.assetstore.before', 'hdfs_assetstore',
                createAssetstore)

    (Assetstore.createAssetstore.description
        .param('machine', 'The NERSC machine name.', required=False)
        .param('baseUrl', 'The NEWT API base URL.', required=False))

    info['apiRoot'].newt = Newt()
