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

#    NB: The current status endpoint seem to be broken on cori so
#        for now fall back to executed squeue manually.
#
#     def job_status(self, job):
#         url = '%s/queue/%s/%s' % (NEWT_BASE_URL, self._machine,
#                                   job['queueJobId'])
#         r = self._session.get(url)
#         check_status(r)
#         json_response = r.json()
#
#         if json_response['status'] != 'OK' or json_response['error']:
#             raise Exception(json_response['error'])
#
#         status = None
#         slurm_status = self._extract_job_status(json_response, job)
#
#         if slurm_status:
#             if slurm_status in SlurmQueueAdapter.RUNNING_STATE:
#                 status = JobQueueState.RUNNING
#             elif slurm_status in SlurmQueueAdapter.ERROR_STATE:
#                 status = JobQueueState.ERROR
#             elif slurm_status in SlurmQueueAdapter.QUEUED_STATE:
#                 status = JobQueueState.QUEUED
#             elif slurm_status in SlurmQueueAdapter.COMPLETE_STATE:
#                 status = JobQueueState.COMPLETE
#
#         return status
#
#     def _extract_job_status(self, job_status_output, job):
#         status = None
#
#         if 'status' in job_status_output:
#             status = job_status_output['status']
#
#         return status
