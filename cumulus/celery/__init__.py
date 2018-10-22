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
from bson.objectid import ObjectId
from celery import Celery
from celery.datastructures import force_mapping
from cumulus.taskflow.utility import find_taskflow_modules
from . import commonconfig, commandconfig, monitorconfig
from kombu.serialization import register
import json
import jsonpickle


def oid_safe_dumps(obj):
    return json.dumps(obj, default=str)


def oid_safe_loads(obj):
    return json.loads(obj)


register('oid_safe_json', oid_safe_dumps, oid_safe_loads,
         content_type='application/x-oid_safe_json',
         content_encoding='utf-8')


# Ensure ObjectIds are converted to string,
# so it will work correctly with the internal serializer
@jsonpickle.handlers.register(ObjectId, base=True)
class ObjectIdHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data = str(obj)
        return data

    def restore(self, data):
        return data


register('jsonpickle', jsonpickle.encode, jsonpickle.decode,
         content_type='application/json',
         content_encoding='utf-8')

_includes = [
    'cumulus.ansible.tasks.cluster',
    'cumulus.ansible.tasks.volume',
    'cumulus.tasks.cluster',
    'cumulus.tasks.job',
    'cumulus.ssh.tasks.key',
    'cumulus.aws.ec2.tasks.key'
]

taskflow_modules = find_taskflow_modules()
_includes += taskflow_modules

# Route short tasks to their own queue
_routes = {
    'cumulus.tasks.job.monitor_job': {
        'queue': 'monitor'
    },
    'cumulus.tasks.job.monitor_jobs': {
        'queue': 'monitor'
    },
    'cumulus.tasks.job.monitor_process': {
        'queue': 'monitor'
    },
    'cumulus.task.status.monitor_status': {
        'queue': 'monitor'
    }
}

command = Celery('command',  backend='amqp',
                 include=_includes)

command.config_from_object(commonconfig)
command.conf.update(force_mapping(commandconfig))
command.conf.update(
    CELERY_ROUTES=_routes,
    CELERY_TASK_SERIALIZER='oid_safe_json',
    CELERY_ACCEPT_CONTENT=('json', 'oid_safe_json', 'jsonpickle'),
    CELERY_RESULT_SERIALIZER='json',
    CELERYD_PREFETCH_MULTIPLIER=1
)

monitor = Celery('monitor',  backend='amqp',
                 include=_includes)

monitor.config_from_object(commonconfig)
monitor.conf.update(force_mapping(monitorconfig))
monitor.conf.update(
    CELERY_ROUTES=_routes,
    CELERY_ACCEPT_CONTENT=('json', 'jsonpickle')
)
