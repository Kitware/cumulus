import json
import cumulus
from jinja2 import Template
import requests
import sys

def _check_status(request):
    if request.status_code != 200:
        print >> sys.stderr, request.content
        request.raise_for_status()


def _template_dict(d, variables):

    json_str = json.dumps(d)
    json_str = Template(json_str).render(**variables)

    return json.loads(json_str)

def _run_http(token, variables,  params):
    headers = {'Girder-Token': token}
    url = '%s%s' % (cumulus.config.girder.baseUrl, params['url'])
    print url
    r = requests.request(params['method'], url, headers=headers)
    _check_status(r)

    if 'output' in params:
        variables[params['output']] = r.json()

def run(token, spec, variables):

    for step in spec['steps']:
        step = _template_dict(step, variables)
        if step['type'] == 'http':
            _run_http(token, variables, step['params'])
