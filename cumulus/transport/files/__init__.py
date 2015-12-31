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
import requests
from jsonpath_rw import parse

import cumulus
from cumulus.common import check_status

ssh_cluster = ['trad', 'ec2']


def get_assetstore_url_base(cluster):
    if cluster['type'] in ssh_cluster:
        return 'sftp_assetstores'
    elif cluster['type'] == 'newt':
        return 'newt_assetstores'
    else:
        raise Exception('Unsupported cluster type')


def get_assetstore_id(girder_token, cluster):
    if 'assetstoreId' not in cluster:
        headers = {'Girder-Token':  girder_token}
        url_base = get_assetstore_url_base(cluster)
        create_url = '%s/%s' % (cumulus.config.girder.baseUrl, url_base)
        body = {
            'name': cluster['_id'],
            'host': cluster['config']['host'],
            'machine': cluster['config']['host'],
            'authKey': cluster['_id']
        }

        user = parse('config.ssh.user').find(cluster)
        if user:
            body['user'] = user[0].value

        r = requests.post(create_url, json=body, headers=headers)
        check_status(r)

        cluster['assetstoreId'] = r.json()['_id']

        cluster_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl,
                                          cluster['_id'])
        body = {
            'assetstoreId': cluster['assetstoreId']
        }
        r = requests.patch(cluster_url, json=body, headers=headers)
        check_status(r)

    return cluster['assetstoreId']
