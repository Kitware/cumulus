import unittest
import os
import glob
import shutil
import tempfile
from cumulus.ansible.tasks.utils import get_playbook_directory
from cumulus.ansible.tasks.utils import run_playbook as _run_playbook
import multiprocessing
import time
import json


def flaskProcess(requests_file):
    from flask import Flask
    from flask import request

    app = Flask(__name__)

    @app.route("/log", methods=["POST"])
    def test():
        with open(requests_file, "a") as fh:
            fh.write((request.data + "\n").encode('utf8'))
        return "SUCCESS"

    app.run(debug=False, use_reloader=False)


class AnsibleRunTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a temporary directory
        cls.tempdir = tempfile.mkdtemp()
        cls.playbookdir = os.path.join(cls.tempdir, "playbooks")

        # Copy in the playbook directory from the source tree
        shutil.copytree(get_playbook_directory(), cls.playbookdir)

        # Copy in test fixtures
        fixtures = os.path.join(os.path.dirname(__file__),
                                "fixtures", "ansible", "*.yml")
        for pth in glob.glob(fixtures):
            shutil.copy(pth, cls.playbookdir)

        # Copy dummy inventory
        shutil.copy(os.path.join(os.path.dirname(__file__),
                                 "fixtures", "ansible", "inventory"),
                    cls.playbookdir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir)

    def setUp(self):
        self.requests_file = os.path.join(self.tempdir, "requests.txt")
        with open(self.requests_file, 'a'):
            os.utime(self.requests_file, None)

        # Launch Flask webserver
        self.webserver = multiprocessing.Process(target=flaskProcess,
                                                 args=(self.requests_file, ))
        self.webserver.daemon = True
        self.webserver.start()
        time.sleep(0.3)

    def tearDown(self):
        if os.path.exists(self.requests_file):
            os.remove(self.requests_file)
        self.webserver.terminate()
        self.webserver.join()

    def run_playbook(self, playbook, extra_vars=None):
        env = os.environ.copy()
        env.update({
            'LOG_WRITE_URL': 'http://localhost:5000/log',
            'GIRDER_TOKEN': 'mock_girder_token',
            'CLUSTER_ID': 'mock_cluster_id'})


        _run_playbook(
            os.path.join(self.playbookdir, playbook),
            inventory=os.path.join(self.playbookdir, "inventory"),
            extra_vars=extra_vars,
            env=env)

        with open(self.requests_file, "rb") as fh:
            contents = fh.read().decode('utf8')

        return [json.loads(l) for l in contents.split("\n") if l != '']

    def test_run(self):
        sources = self.run_playbook("test_run_playbook.yml")
        targets = [{"status": "starting",
                    "data": {},
                    "msg": "Test Playbook",
                    "type": "play"},
                   {"status": "starting",
                    "data": {},
                    "msg": "",
                    "type": "task"},
                   {"status": "finished",
                    "data":  {"changed": False,
                              "msg": "Works!",
                              "_ansible_verbose_always": True,
                              "_ansible_no_log": False},
                    "msg": "",
                    "type": "task"},
                   {"status": "finished",
                    "data": {},
                    "msg": "Test Playbook",
                    "type": "play"}]




        # Same length
        self.assertEquals(len(sources), len(targets))

        for source, target in zip(sources, targets):

            # target keys are a subset of source
            self.assertTrue(set(target.keys()) <= set(source.keys()))

            # target and source values are equal
            for key in target.keys():
                self.assertEquals(source[key], target[key])
