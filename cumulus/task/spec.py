import jsonschema
import json
import inspect
import os
import cumulus.task

def validate(spec):
    path = inspect.getsourcefile(cumulus.task)
    schema_path = os.path.join(os.path.dirname(path), 'task.json')
    with open(schema_path, 'r') as fp:
        schema = json.load(fp)

    jsonschema.validate(spec, schema)
