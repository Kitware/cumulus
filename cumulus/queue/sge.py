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
from cumulus.queue.abstract import AbstractQueueAdapter


class SgeQueueAdapter(AbstractQueueAdapter):
    # Running states
    RUNNING_STATE = ['r', 'd', 'e']

    # Queued states
    QUEUED_STATE = ['qw', 'q', 'w', 's', 'h', 't']

    def terminate_job(self, job):
        command = 'qdel %s' % job['queueJobId']
        output = self._cluster_connection.execute(command)

        return output

    def _parse_job_id(self, submit_output):
        m = re.match('^[Yy]our job (\\d+)', submit_output[0])
        if not m:
            raise Exception('Unable to extraction job id from: %s'
                            % submit_output[0])
        sge_id = m.group(1)

        return sge_id

    def submit_job(self, job, job_script):
        command = 'cd %s && qsub -cwd ./%s' % (job['dir'], job_script)
        output = self._cluster_connection.execute(command)

        if len(output) != 1:
            raise Exception('Unexpected qsub output: %s' % output)

        return self._parse_job_id(output)

    def job_status(self, job):
        output = self._cluster_connection.execute('qstat')

        return self._extract_job_status(output, job)

    def _extract_job_status(self, job_status_output, job):
        state = None
        job_id = job[AbstractQueueAdapter.QUEUE_JOB_ID]
        for line in job_status_output:
            m = re.match('^\\s*(\\d+)\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\w+)',
                         line)
            if m and m.group(1) == job_id:
                state = m.group(2)

        return state

    def is_running(self, state):
        return state in SgeQueueAdapter.RUNNING_STATE

    def is_queued(self, state):
        return state in SgeQueueAdapter.QUEUED_STATE
