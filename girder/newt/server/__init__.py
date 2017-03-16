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
from girder.constants import AssetstoreType, AccessType
from girder.utility.model_importer import ModelImporter
from girder.utility.assetstore_utilities import setAssetstoreAdapter
from .rest import Newt, NewtAssetstore, create_assetstore
from .constants import NEWT_BASE_URL


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


def create_assetstore_from_event(event):
    params = event.info['params']
    if params.get('type') == AssetstoreType.NEWT:
        event.addResponse(create_assetstore(params))
        event.preventDefault()


def load(info):

    AssetstoreType.NEWT = 'newt'
    setAssetstoreAdapter(AssetstoreType.NEWT, NewtAssetstoreAdapter)
    events.bind('assetstore.update', 'newt', updateAssetstore)

    info['apiRoot'].newt = Newt()
    info['apiRoot'].newt_assetstores = NewtAssetstore()
