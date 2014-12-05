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

    body = None

    if 'body' in params:
        body = params['body']

    r = requests.request(params['method'], url, json=body, headers=headers)
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

def run(token, task, spec, variables, start_step=0):
    headers = {'Girder-Token': token}
    steps = spec['steps']
    for s in range(start_step, len(steps)):
        step = _template_dict(steps[s], variables)
        if step['type'] == 'http':
            _run_http(token, variables, step['params'])
        elif step['type'] == 'status':
            spec['steps'][s] = step
            _run_status(token, task, spec, s, variables)
            return

    # Task is now complete, save the variable into the output property and set
    # status
    url = '%s/tasks/%s' % (cumulus.config.girder.baseUrl, task['_id'])
    update = {
        'output': variables,
        'status': 'complete'
    }
    r = requests.patch(url, headers=headers, json=update)
    _check_status(r)


