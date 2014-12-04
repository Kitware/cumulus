import json
import cumulus
from jinja2 import Template
import requests
import sys
from cumulus.starcluster.tasks.celery import app


def _check_status(request):
    if request.status_code != 200:
        print >> sys.stderr, request.content
        request.raise_for_status()


def _template_dict(d, variables):

    json_str = json.dumps(d)
    json_str = Template(json_str).render(**variables)

    return json.loads(json_str)

def _run_http(token, variables, params):
    headers = {'Girder-Token': token}
    url = '%s%s' % (cumulus.config.girder.baseUrl, params['url'])
    print url
    r = requests.request(params['method'], url, headers=headers)
    _check_status(r)

    if 'output' in params:
        variables[params['output']] = r.json()

def _run_status(token, task, spec, step, variables):
    # Fire of task to monitor the status
    print task
    print spec
    print step
    print token

    task['_id'] = str(task['_id'])

    app.send_task('cumulus.task.status.monitor_status', args=(token, task, spec, step, variables))

def run(token, task, spec, variables, step=0):

    steps = spec['steps']
    for s in range(step, len(steps)):
        step = _template_dict(steps[s], variables)
        if step['type'] == 'http':
            _run_http(token, variables, step['params'])
        elif step['type'] == 'status':
            _run_status(token, task, spec, s, variables)

