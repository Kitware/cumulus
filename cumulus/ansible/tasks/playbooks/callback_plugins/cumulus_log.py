from __future__ import print_function
import requests
import cumulus
from cumulus.common import get_post_logger
import os
import sys

from ansible.plugins.callback import CallbackBase

STARTING = 'starting'
SKIPPED = 'skipped'
FINISHED = 'finished'
UNREACHABLE = 'unreachable'
ERROR = 'error'
WARNING = 'warning'
INFO = 'info'


class CallbackModule(CallbackBase):

    """
    """

    def __init__(self):
        super(CallbackModule, self).__init__()
        self.current_task = None
        self.current_play = None
        self.logger = get_post_logger('cumulus_log', self.girder_token,
                                      self.log_write_url)

    @property
    def cluster_id(self):
        return os.environ.get('CLUSTER_ID')

    @property
    def girder_token(self):
        return os.environ.get('GIRDER_TOKEN')

    @property
    def log_write_url(self):
        return os.environ.get('LOG_WRITE_URL')

    def log(self, status, message, type='task', data=None):

        data = {} if data is None else data

        invocation = data.pop('invocation', None)
        if invocation is not None:
            data['module_name'] = invocation['module_name']

        if self.log_write_url is not None and \
           self.girder_token is not None:
            msg = {'status': status,
                   'type': type,
                   'data': data}

            if status == ERROR:
                self.logger.error(message, extra=msg)
            if status in [UNREACHABLE, WARNING]:
                self.logger.warn(message, extra=msg)
            else:
                self.logger.info(message, extra=msg)

    def runner_on_failed(self, host, res, ignore_errors=False):
        if self.cluster_id is not None and \
           self.girder_token is not None:

            level = ERROR
            if ignore_errors:
                level = INFO

            self.log(level, self.current_task, data=res)

            if ignore_errors:
                return

            # Update girder with the new status
            status_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl,
                                             self.cluster_id)
            headers = {'Girder-Token':  self.girder_token}
            updates = {
                'status': 'error'
            }

            r = requests.patch(status_url, headers=headers, json=updates)
            if r.status_code != 200:
                print(r.content, file=sys.stderr)
                r.raise_for_status()

    def runner_on_ok(self, host, res):
        self.log(FINISHED, self.current_task, data=res)

    def runner_on_skipped(self, host, item=None):
        res = {'host': host}
        self.log(SKIPPED, self.current_task, data=res)

    def runner_on_unreachable(self, host, res):
        self.log(UNREACHABLE, self.current_task, data=res)

    def playbook_on_task_start(self, name, is_conditional):
        self.current_task = name
        self.log(STARTING, name)

    def playbook_on_play_start(self, name):
        if self.current_play is not None:
            self.log(FINISHED, self.current_play, type='play')

        self.current_play = name
        self.log(STARTING, name, type='play')

    def playbook_on_stats(self, stats):
        if self.current_play is not None:
            self.log(FINISHED, self.current_play, type='play')
