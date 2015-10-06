from __future__ import absolute_import
from cumulus.celery import monitor
from cumulus.common import check_status
import cumulus
import requests
import traceback
from . import runner
from celery.exceptions import MaxRetriesExceededError

sleep_interval = 5


def _add_log_entry(token, task, entry):
    headers = {'Girder-Token': token}
    url = '%s/tasks/%s/log' % (cumulus.config.girder.baseUrl, task['_id'])
    r = requests.post(url, headers=headers, json=entry)
    check_status(r)


def _update_status(headers, task, status):
    update = {
        'status': status
    }
    url = '%s/tasks/%s' % (cumulus.config.girder.baseUrl, task['_id'])
    r = requests.patch(url, headers=headers, json=update)
    check_status(r)


@monitor.task(bind=True, max_retries=None)
def monitor_status(celery_task, token, task, spec, step, variables):
    headers = {'Girder-Token':  token}
    max_retries = None
    try:
        steps = spec['steps']
        status_step = steps[step]
        params = status_step['params']
        if 'timeout' in params:
            timeout = int(params['timeout'])
            max_retries = timeout % sleep_interval

        url = '%s%s' % (cumulus.config.girder.baseUrl, params['url'])
        status = requests.get(url, headers=headers)
        check_status(status)
        status = status.json()
        selector = params['selector']
        selector_path = selector.split('.')
        for key in selector_path:
            if key in status:
                status = status.get(key)
            else:
                raise Exception('Unable to extract status from \'%s\''
                                ' using \'%s\'' % (status, selector))

        if status in params['success']:
            runner.run(token, task, spec, variables, step + 1)
        elif status in params['failure']:
            _update_status(headers, task, 'failure')
        else:
            celery_task.retry(throw=False, countdown=sleep_interval,
                              max_retries=max_retries)

    except MaxRetriesExceededError:
        _update_status(headers, task, 'timeout')
    except BaseException as ex:
        # Update task log
        entry = {
            'msg': ex.message,
            'stack': traceback.format_exc()
        }
        _add_log_entry(token, task, entry)
        traceback.print_exc()
