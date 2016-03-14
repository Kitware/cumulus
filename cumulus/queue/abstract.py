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


class AbstractQueueAdapter(object):
    QUEUE_JOB_ID = 'queueJobId'

    def __init__(self, cluster, cluster_connection):
        self._cluster = cluster
        self._cluster_connection = cluster_connection

    def submit_job(self, job, job_script):
        raise NotImplementedError('Subclasses should implement this')

    def terminate_job(self, job):
        raise NotImplementedError('Subclasses should implement this')

    def job_statuses(self, jobs):
        raise NotImplementedError('Subclasses should implement this')
