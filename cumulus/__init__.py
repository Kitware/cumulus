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
