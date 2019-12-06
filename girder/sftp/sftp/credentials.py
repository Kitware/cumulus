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
from jsonpath_rw import parse

from girder.utility.model_importer import ModelImporter
from girder.constants import AccessType
from girder.api.rest import getCurrentUser

import cumulus
from cumulus.common.jsonpath import get_property

def retrieve_credentials(event):
    cluster_id = event.info['authKey']
    user = event.info['user']
    model = ModelImporter.model('cluster', 'cumulus')
    cluster = model.load(cluster_id, user=getCurrentUser(),
                         level=AccessType.READ)
    event.stopPropagation()

    if not cluster:
        return

    username = parse('config.ssh.user').find(cluster)[0].value
    key_name = parse('config.ssh.key').find(cluster)[0].value
    key_path = os.path.join(cumulus.config.ssh.keyStore, key_name)
    passphrase = get_property('config.ssh.passphrase', cluster)

    if user != username:
        raise Exception('User doesn\'t match cluster user id ')

    event.addResponse((key_path, passphrase))
