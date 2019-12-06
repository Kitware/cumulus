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

import re

from .assetstore import NewtAssetstoreAdapter
import girder
from girder import events
from girder.api import access
from girder.api.v1.assetstore import Assetstore
from girder.constants import AssetstoreType, AccessType
from girder.utility.assetstore_utilities import setAssetstoreAdapter
from girder.models.model_base import ValidationException
from girder.models.user import User
from girder.utility import setting_utilities
from girder.plugin import GirderPlugin

from .rest import Newt, NewtAssetstore, create_assetstore
from .constants import NEWT_BASE_URL, PluginSettings


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


@setting_utilities.validator(PluginSettings.IGNORE_REGISTRATION_POLICY)
def validateIgnoreRegistrationPolicy(doc):
    if not isinstance(doc['value'], bool):
        raise ValidationException('Ignore registration policy setting must be boolean.', 'value')


class NewtPlugin(GirderPlugin):
    DISPLAY_NAME = 'NEWT plugin'

    def load(self, info):
        AssetstoreType.NEWT = 'newt'
        setAssetstoreAdapter(AssetstoreType.NEWT, NewtAssetstoreAdapter)
        events.bind('assetstore.update', 'newt', updateAssetstore)

        info['apiRoot'].newt = Newt()
        info['apiRoot'].newt_assetstores = NewtAssetstore()

        if hasattr(girder, '__version__') and girder.__version__[0] == '3':
            # Replace User._validateLogin to accept 3-letter user names
            def _validateNewtLogin(login):
                if '@' in login:
                    # Hard-code this constraint so we can always easily distinguish
                    # an email address from a login
                    raise ValidationException('Login may not contain "@".', 'login')

                # For reference, girder's regex is r'^[a-z][\da-z\-\.]{3,}$'
                if not re.match(r'^[a-z][\da-z_\-\.]{2,}$', login):
                    raise ValidationException(
                        'Login must be at least 3 characters, start with a letter, and may only contain '
                        'letters, numbers, underscores, dashes, and periods.', 'login')

            User()._validateLogin = _validateNewtLogin
