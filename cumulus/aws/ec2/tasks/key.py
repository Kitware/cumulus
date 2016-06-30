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
import requests
import traceback
import stat

import cumulus

from cumulus.celery import command
from cumulus.common import check_status
from cumulus.aws.ec2 import get_ec2_client


def _key_path(profile):
    return os.path.join(cumulus.config.ssh.keyStore, str(profile['_id']))


@command.task
def generate_key_pair(aws_profile, girder_token):
    try:
        client = get_ec2_client(aws_profile)
        key_path = _key_path(aws_profile)
        key_pair = client.create_key_pair(KeyName=aws_profile['_id'])

        with open(key_path, 'wb') as fp:
            fp.write(key_pair['KeyMaterial'].encode('utf8'))
        os.chmod(key_path, stat.S_IRUSR)

        aws_profile['status'] = 'available'

    except Exception as ex:
        aws_profile['status'] = 'error'
        aws_profile['errorMessage'] = '%s: %s' % (type(ex).__name__, ex)
        traceback.print_exc()

    update_url = '%s/user/%s/aws/profiles/%s' % (cumulus.config.girder.baseUrl,
                                                 aws_profile['userId'],
                                                 aws_profile['_id'])

    headers = {'Girder-Token':  girder_token}
    r = requests.patch(update_url, json=aws_profile, headers=headers)
    check_status(r)


@command.task
def delete_key_pair(aws_profile, girder_token):
    path = _key_path(aws_profile)

    if os.path.exists(path):
        os.remove(path)
