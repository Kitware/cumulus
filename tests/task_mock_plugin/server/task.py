import time
import mock
import threading

from girder.api import access

from cumulus.task.status import monitor_status


import girder.plugins.task

thread_local = threading.local()

def _mock_retry(args=None, kwargs=None, exc=None, throw=True, eta=None,
                countdown=None, max_retries=None, **options):

    time.sleep(1)
    with mock.patch('cumulus.starcluster.tasks.celery.monitor.Task.retry', _mock_retry), \
         mock.patch('cumulus.task.runner._run_status', _mock_run_status):
        monitor_status(*thread_local.args)


def _mock_run_status(token, task, spec, step, variables):
    print '_mock_run_status'
    # Fire of task to monitor the status
    task['_id'] = str(task['_id'])

    def run_monitor_status():
        thread_local.args = (token, task, spec, step, variables)
        with mock.patch('cumulus.starcluster.tasks.celery.monitor.Task.retry', _mock_retry):
            monitor_status(token, task, spec, step, variables)

    t = threading.Thread(target=run_monitor_status)
    t.start()

class Task(girder.plugins.task.Task):

    @access.user
    def run(self, id, params):

        with mock.patch('cumulus.starcluster.tasks.celery.monitor.Task.retry', _mock_retry), \
             mock.patch('cumulus.task.runner._run_status', _mock_run_status):

            super(Task, self).run(id, params)




