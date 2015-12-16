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

import cumulus.starcluster.logging
from cumulus.common import check_status
from cumulus.starcluster.common import get_ssh_connection
from cumulus.celery import command
import starcluster.logger


@command.task
@cumulus.starcluster.logging.capture
def test_connection(cluster, log_write_url=None, girder_token=None):
    cluster_id = cluster['_id']
    cluster_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl, cluster_id)
    log = starcluster.logger.get_starcluster_logger()
    headers = {'Girder-Token':  girder_token}

    try:
        # First fetch the cluster with this 'admin' token so we get the
        # passphrase filled out.
        r = requests.get(cluster_url, headers=headers)
        check_status(r)
        cluster = r.json()

        with get_ssh_connection(girder_token, cluster) as ssh:
            status = 'running'
            # For simply test can we can connect to cluster and
            # qsub is installed
            output = ssh.execute('command -v qsub')
        if len(output) < 1:
            log.error('Unable to find qsub on cluster')
            status = 'error'

        r = requests.patch(
            cluster_url, headers=headers, json={'status': status})
        check_status(r)
    except Exception as ex:
        r = requests.patch(cluster_url, headers=headers,
                           json={'status': 'error'})
        # Log the error message
        log.exception(ex)
