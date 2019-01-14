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

import json
from jsonschema import validate
from easydict import EasyDict as edict
import pkg_resources as pr
from .version import __version__  # noqa


__license__ = 'Apache 2.0'

config_schema = {
    'type': 'object',
    'required': ['girder'],
    'properties': {
        'girder': {
            'type': 'object',
            'required': ['baseUrl', 'user', 'group'],
            'properties': {
                'baseUrl': {
                    'type': 'string',
                    'pattern': r'^http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]'
                    r'|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+$'
                },
                'user': {
                    'type': 'string'
                },
                'group': {
                    'type': 'string'
                }
            }
        },
        'moadReader': {
            'type': 'object',
            'required': ['pluginPath'],
            'properties': {
                'pluginPath': {
                    'type': 'string'
                }
            }
        },
        'taskFlow': {
            'type': 'object',
            'required': ['pluginPath'],
            'properties': {
                'path': {
                    'type': 'array',
                    'items': {
                        'type': 'string'
                    }
                }
            }
        }
    }
}

if pr.resource_exists(__name__, 'conf/config.json'):
    config = json.loads(
        pr.resource_string(__name__, 'conf/config.json').decode('utf8'))
else:
    config = json.loads(
        pr.resource_string(__name__, 'conf/default_config.json').decode('utf8'))

validate(config, config_schema)

config = edict(config)
