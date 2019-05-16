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

from girder import events
from girder.api import access
from girder.api.v1.assetstore import Assetstore
from girder.constants import AssetstoreType
from girder.plugin import GirderPlugin
from girder.utility.model_importer import ModelImporter
from girder.utility.assetstore_utilities import setAssetstoreAdapter

from .assetstore import SftpAssetstoreAdapter
from .credentials import retrieve_credentials
from .rest import SftpAssetstoreResource

def getAssetstore(event):
    assetstore = event.info
    if assetstore['type'] == AssetstoreType.SFTP:
        event.stopPropagation()
        event.addResponse(SftpAssetstoreAdapter)


def updateAssetstore(event):
    params = event.info['params']
    assetstore = event.info['assetstore']

    if assetstore['type'] == AssetstoreType.SFTP:
        assetstore[AssetstoreType.SFTP] = {
            'host': params.get('host', assetstore['sftp']['host']),
            'user': params.get('host', assetstore['sftp']['user']),
            'keystore': params.get('host', assetstore['sftp']['keystore'])
        }


class SftpPlugin(GirderPlugin):
    DISPLAY_NAME = 'SFTP plugin'

    def load(self, info):
        AssetstoreType.SFTP = 'sftp'
        setAssetstoreAdapter(AssetstoreType.SFTP, SftpAssetstoreAdapter)
        events.bind('assetstore.update', 'sftp', updateAssetstore)
        events.bind('assetstore.sftp.credentials.get', 'sftp', retrieve_credentials)

        info['apiRoot'].sftp_assetstores = SftpAssetstoreResource()
