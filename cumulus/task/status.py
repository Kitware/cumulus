from __future__ import absolute_import
from cumulus.starcluster.logging import StarClusterLogHandler, StarClusterCallWriteHandler, logstdout, StarClusterLogFilter
import cumulus.starcluster.logging
from cumulus.starcluster.tasks.celery import app
from cumulus.starcluster.tasks.common import _check_status
import cumulus
import requests
import os
import sys
import re
import traceback
from . import runner
from celery.exceptions import MaxRetriesExceededError

sleep_interval = 5

def _update_status(headers, task, status):
    task['status'] = status
    url = '%s/task/%s' % (cumulus.config.girder.baseUrl, task['_id'])
    r = requests.patch(url, headers=headers, json=task)
    _check_status(r)

@app.task(bind=True, max_retries=None)
def monitor_status(token, task, spec, step, variables):
    headers = {'Girder-Token':  token}

    try:
        steps = spec['steps']
        status_step = steps[step]
        if 'timeout' in status_step['params']:
            timeout = int(status_step['params']['timeout'])
            max_retries = timeout % sleep_interval
            task.max_retries = max_retries

        next_step = None
        if step + 1 < len(steps):
            next_step = step+1

        url = '%s/%s' % (cumulus.config.girder.baseUrl, status_step['params']['url'])
        status = requests.get(url, headers=headers)
        _check_status(status)

        selector = status_step['params']['selector']
        selector_path = selector.split('.')
        for key in selector_path:
            if key in status:
                status = status.get(key)
            else:
                raise Exception('Unable to extract status from \'%s\' using \'%s\'' % (status, selector))

        if status in status_step['success']:
            if next_step:
                runner.run(token, task, spec, variables, next_step)
            else:
                _update_status(headers, task, 'complete')
        elif status in status_step['failure']:
            _update_status(headers, task, 'failure')
        else:
            task.retry(throw=False, countdown=sleep_interval)

    except MaxRetriesExceededError:
        _update_status(headers, task, 'timeout')
    except:
        # Update task log
        traceback.print_exc()
