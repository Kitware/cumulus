import requests
import datetime as dt

STARTING = 'starting'
SKIPPED = 'skipped'
FINISHED = 'finished'
UNREACHABLE = 'unreachable'
ERROR = 'error'
WARNING = 'warning'


class CallbackModule(object):

    """
    """

    def __init__(self):
        # from pudb.remote import set_trace; set_trace(term_size=(185, 46))

        self.current_task = None
        self.current_play = None

    @property
    def girder_token(self):
        return self.playbook.extra_vars['girder_token']

    @property
    def log_write_url(self):
        return self.playbook.extra_vars['log_write_url']

    def log(self, status, message, type='task', data=None):
        logged_at = dt.datetime.now().isoformat()
        msg = {"status": status,
               "created": logged_at,
               "type": type,
               "message": message,
               "data": data}

        r = requests.post(self.log_write_url,
                          json=msg,
                          headers={
                              'Girder-Token': self.girder_token,
                              'Content-Type': 'application/json'
                          })

        assert r.status_code == 200

    def on_any(self, *args, **kwargs):
        pass

    def _filter_res(self, res):
        res2 = res.copy()
        res2['module_name'] = res2['invocation']['module_name']
        res2.pop('invocation', None)
        return res2

    def runner_on_failed(self, host, res, ignore_errors=False):
        res2 = self._filter_res(res)
        res2['host'] = host
        self.log(ERROR, self.current_task, data=res2)

    def runner_on_ok(self, host, res):
        res2 = self._filter_res(res)
        res2['host'] = host
        self.log(FINISHED, self.current_task, data=res2)

    def runner_on_skipped(self, host, item=None):
        res2 = {'host': host}
        self.log(SKIPPED, self.current_task, data=res2)

    def runner_on_unreachable(self, host, res):
        res2 = self._filter_res(res)
        res2['host'] = host
        self.log(UNREACHABLE, self.current_task, data=res2)

    def runner_on_no_hosts(self):
        pass

    def runner_on_async_poll(self, host, res, jid, clock):
        pass

    def runner_on_async_ok(self, host, res, jid):
        pass

    def runner_on_async_failed(self, host, res, jid):
        pass

    def playbook_on_start(self):
        pass

    def playbook_on_notify(self, host, handler):
        pass

    def playbook_on_no_hosts_matched(self):
        pass

    def playbook_on_no_hosts_remaining(self):
        pass

    def playbook_on_task_start(self, name, is_conditional):
        self.current_task = name
        self.log(STARTING, name)
        pass

    def playbook_on_vars_prompt(self, varname, private=True, prompt=None,
                                encrypt=None, confirm=False, salt_size=None,
                                salt=None, default=None):
        pass

    def playbook_on_setup(self):
        pass

    def playbook_on_import_for_host(self, host, imported_file):
        pass

    def playbook_on_not_import_for_host(self, host, missing_file):
        pass

    def playbook_on_play_start(self, name):
        if self.current_play is not None:
            self.log(FINISHED, self.current_play, type='play')

        self.current_play = name
        self.log(STARTING, name, type='play')
        pass

    def playbook_on_stats(self, stats):
        if self.current_play is not None:
            self.log(FINISHED, self.current_play, type='play')

        pass
