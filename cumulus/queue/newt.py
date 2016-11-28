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
import requests

from cumulus.queue.slurm import SlurmQueueAdapter, AbstractQueueAdapter
from cumulus.common import check_status
from cumulus.transport.newt import NEWT_BASE_URL


class NewtQueueAdapter(SlurmQueueAdapter):
    def __init__(self, cluster, cluster_connection):
        super(NewtQueueAdapter, self).__init__(cluster, cluster_connection)
        self._session = requests.Session()
        self._session.cookies.set('newt_sessionid',
                                  cluster_connection.session_id)
        self._machine = parse('config.host').find(cluster)[0].value

    def terminate_job(self, job):
        url = '%s/queue/%s/%s' % (NEWT_BASE_URL,
                                  self._machine, job['queueJobId'])
        r = self._session.delete(url)
        check_status(r)
        json_response = r.json()

        if json_response['status'] != 'OK' or json_response['error']:
            raise Exception(json_response['error'])

    def submit_job(self, job, job_script):
        url = '%s/queue/%s' % (NEWT_BASE_URL, self._machine)
        job_file_path = os.path.join(job['dir'], job_script)
        data = {
            'jobfile': job_file_path
        }

        r = self._session.post(url, data=data)
        check_status(r)
        json_response = r.json()

        if json_response['status'] != 'OK' or 'jobid' not in json_response:
            raise Exception(json_response['error'])

        return json_response['jobid']

    def job_statuses(self, jobs):
        user = parse('config.user').find(self._cluster)

        if not user:
            raise Exception('Unable to extract user from cluster '
                            'configuration.')

        user = user[0].value
        url = '%s/queue/%s?user=%s' % (NEWT_BASE_URL, self._machine, user)
        r = self._session.get(url)
        check_status(r)
        json_response = r.json()

        states = []
        for job in jobs:
            slurm_state = self._extract_job_status(json_response, job)
            state = self.to_job_queue_state(slurm_state)
            states.append((job, state))

        return states

    def _extract_job_status(self, response, job):
        status = None

        for job_entry in response:
            if job_entry['jobid'] == job[AbstractQueueAdapter.QUEUE_JOB_ID] and\
                    'status' in job_entry:
                status = job_entry['status']
                break

        return status
