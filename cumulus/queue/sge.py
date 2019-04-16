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
from cumulus.constants import JobQueueState


class SgeQueueAdapter(AbstractQueueAdapter):
    # Running states
    RUNNING_STATE = ['r', 'd']

    ERROR_STATE = ['e']

    # Queued states
    QUEUED_STATE = ['qw', 'q', 'w', 's', 'h', 't']

    def terminate_job(self, job):
        command = 'qdel %s' % job['queueJobId']
        output = self._cluster_connection.execute(command)

        return output

    def _parse_job_id(self, submit_output):
        m = re.match(r'^[Yy]our job (\d+)', submit_output[0])
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

    def job_statuses(self, jobs):
        output = self._cluster_connection.execute('qstat')

        states = []
        for job in jobs:
            state = None
            sge_state = self._extract_job_status(output, job)

            if sge_state:
                if sge_state in SgeQueueAdapter.RUNNING_STATE:
                    state = JobQueueState.RUNNING
                elif sge_state in SgeQueueAdapter.ERROR_STATE:
                    state = JobQueueState.ERROR
                elif sge_state in SgeQueueAdapter.QUEUED_STATE:
                    state = JobQueueState.QUEUED

            states.append((job, state))

        return states

    def _extract_job_status(self, job_status_output, job):
        state = None
        job_id = job[AbstractQueueAdapter.QUEUE_JOB_ID]
        for line in job_status_output:
            m = re.match(r'^\s*(\d+)\s+\S+\s+\S+\s+\S+\s+(\w+)',
                         line)
            if m and m.group(1) == job_id:
                state = m.group(2).lower()
                break

        return state

    def number_of_slots(self, parallel_env):
        slots = -1
        output = self._cluster_connection.execute('qconf -sp %s' % parallel_env)

        for line in output:
            m = re.match(r'slots[\s]+(\d+)', line)
            if m:
                slots = m.group(1)
                break

        if int(slots) < 1:
            raise Exception('Unable to retrieve number of slots')

        return slots
