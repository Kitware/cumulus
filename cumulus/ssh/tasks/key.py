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

import random
import string
import os
import stat
from paramiko.rsakey import RSAKey
import requests


import cumulus
from cumulus.celery import command
from cumulus.common import check_status, get_cluster_logger


def _key_path(profile):
    return os.path.join(cumulus.config.ssh.keyStore, str(profile['_id']))


@command.task
def generate_key_pair(cluster, girder_token=None):
    """
    Task to generate a new key pair for a user.
    """
    cluster_id = cluster['_id']
    status_url = '%s/clusters/%s' \
        % (cumulus.config.girder.baseUrl, cluster_id)
    log = get_cluster_logger(cluster, girder_token)
    headers = {'Girder-Token':  girder_token}

    try:
        new_key = RSAKey.generate(bits=4096)
        passphrase = ''.join(random.SystemRandom()
                             .choice(string.ascii_uppercase
                                     + string.digits) for _ in range(64))
        key_path = os.path.join(cumulus.config.ssh.keyStore, cluster_id)

        new_key.write_private_key_file(key_path, password=passphrase)
        # Allow group read as well
        os.chmod(key_path, stat.S_IREAD | stat.S_IWRITE | stat.S_IRGRP)

        comment = 'cumulus generated access key'
        public_key = '%s %s %s' % (new_key.get_name(), new_key.get_base64(),
                                   comment)

        # Update passphrase and public key on cluster model
        config_update = {
            'config': {
                'ssh': {
                    'passphrase': passphrase,
                    'publicKey': public_key
                }
            },
            'status': 'created'
        }

        patch_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl,
                                        cluster_id)
        request = requests.patch(patch_url, json=config_update, headers=headers)
        check_status(request)
    except Exception as ex:
        r = requests.patch(status_url, headers=headers,
                           json={'status': 'error'})
        check_status(r)
        # Log the error message
        log.error(ex)


@command.task
def delete_key_pair(aws_profile, girder_token):
    path = _key_path(aws_profile)

    if os.path.exists(path):
        os.remove(path)
