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
import inspect
from jsonschema import validate
import os
from easydict import EasyDict as edict


config_schema = {
    "type": "object",
    "required": ["girder"],
    "properties": {
        "girder": {
            "type": "object",
            "required": ["baseUrl", "user", "group"],
            'properties': {
                "baseUrl": {
                    "type": "string",
                    "pattern": "^http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]"
                    "|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+$"
                },
                "user": {
                    "type": "string"
                },
                "group": {
                    "type": "string"
                }
            }
        },
        "moadReader": {
            "type": "object",
            "required": ["pluginPath"],
            'properties': {
                "pluginPath": {
                    "type": "string"
                }
            }
        }
    }
}

module_dir = os.path.dirname(os.path
                             .abspath(inspect.getfile(inspect.currentframe())))
config_path = os.path.join(module_dir, '..', 'config.json')

if not os.path.exists(config_path):
    config_path = os.path.join(module_dir, '..', 'default_config.json')

with open(config_path, 'r') as fp:
    config = json.load(fp)

validate(config, config_schema)

config = edict(config)
