from jsonschema import validate
import json
import requests
import cumulus
from jinja2 import Template

def _template_dict(d, variables):

    json_str = json.dumps(d)
    json_str = Template(json_str).render(**variables)

    return json.loads(json_str)

def run_http(variable, token, params):
    headers = {'Girder-Token': token}
    url = '%s/%s' % (cumulus.config.girder.baseUrl, params['url'])
    requests.request(params['method'], url, headers=headers)

def run(spec, variables):

    spec = _template_dict(spec, variables)

    for step in spec['steps']:
        print step
        if step['type'] == 'http':
            run_http(variables, 'token', step['params'])


with open('task.json', 'r') as fp:
    schema = json.load(fp)

with open('test.json', 'r') as fp:
    test = json.load(fp)

validate(test, schema)


variables = {
    'mesh': {
        'id': 'test mesh id'
    },
    'output': {
        'itemId': 'itemId',
        'name': 'name'
    }
}

run(test, variables)



