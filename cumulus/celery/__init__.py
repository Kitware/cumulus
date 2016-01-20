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
    'cumulus.mytaskflow'
)

# Route short tasks to their own queue
_routes = {
    'cumulus.starcluster.tasks.job.monitor_job': {
        'queue': 'monitor'
    },
    'cumulus.starcluster.tasks.job.monitor_process': {
        'queue': 'monitor'
    },
    'cumulus.task.status.monitor_status': {
        'queue': 'monitor'
    }
}

command = Celery('command',  backend='amqp',
                 broker='amqp://guest:guest@localhost:5672/',
                 include=_includes)

command.config_from_object('cumulus.celery.commandconfig')
command.conf.update(
    CELERY_ROUTES=_routes
)

monitor = Celery('monitor',  backend='amqp',
                 broker='amqp://guest:guest@localhost:5672/',
                 include=_includes)

monitor.config_from_object('cumulus.celery.monitorconfig')
monitor.conf.update(
    CELERY_ROUTES=_routes
)
