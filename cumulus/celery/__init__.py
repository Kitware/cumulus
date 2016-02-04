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

from __future__ import absolute_import
from celery import Celery

_includes = (
    'cumulus.starcluster.tasks.cluster',
    'cumulus.starcluster.tasks.job',
    'cumulus.task.status',
    'cumulus.ssh.tasks.key',
    'cumulus.aws.ec2.tasks.key',
    'cumulus.mytaskflows'
)

# Route short tasks to their own queue
monitor = {'queue': 'monitor'}

_routes = {
    'cumulus.starcluster.tasks.job.monitor_job': monitor,
    'cumulus.starcluster.tasks.job.monitor_process': monitor,
    'cumulus.task.status.monitor_status': monitor
}

app = Celery('cumulus',  backend='amqp',
             broker='amqp://guest:guest@localhost:5672/',
             include=_includes)

app.conf.update(
    CELERY_DEFAULT_EXCHANGE_TYPE='topic',
    CELERY_QUEUES={
        'celery': {
            'routing_key': 'celery'
        },
        'monitor': {
            'routing_key': 'monitor.#'
        },
        'taskflow': {
            'routing_key': 'taskflow.#'
        }
    },
    CELERY_ROUTES=_routes,
    CELERY_TASK_SERIALIZER = 'json',
    CELERY_ACCEPT_CONTENT = ('json',),
    CELERY_RESULT_SERIALIZER = 'json',
    CELERY_ACKS_LATE = True,
    CELERYD_PREFETCH_MULTIPLIER = 1
)
