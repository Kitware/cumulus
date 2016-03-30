import unittest
import os
import glob
import shutil
import tempfile
from cumulus.ansible.tasks.cluster import get_playbook_directory
from cumulus.ansible.tasks.cluster import run_playbook as _run_playbook
import requests
import multiprocessing
import time
import json

def flaskProcess(requests_file):
    from flask import g
    from flask import Flask
    from flask import request

    import logging
    from logging.handlers import SysLogHandler

    logger = logging.getLogger("flask_process")
    logger.setLevel(logging.INFO)

    sysh = SysLogHandler(address=('localhost', 514))
    sysh.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    sysh.setFormatter(formatter)

    logger.addHandler(sysh)

    app = Flask(__name__)

    @app.route("/log", methods=["POST"])
    def test():
        with open(requests_file, "a") as fh:
            fh.write(request.data + "\n")
        return "SUCCESS"

    app.run(debug=False, use_reloader=False)


class AnsibleRunTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a temporary directory
        tempd = tempfile.mkdtemp()
        cls.tempdir = os.path.join(tempd, "playbooks")

        # Copy in the playbook directory from the source tree
        shutil.copytree(get_playbook_directory(), cls.tempdir)

        # Copy in test fixtures
        fixtures = os.path.join(os.path.dirname(__file__),
                                "fixtures", "ansible", "*.yml")
        for pth in glob.glob(fixtures):
            shutil.copy(pth, cls.tempdir)

        # Copy dummy inventory
        shutil.copy(os.path.join(os.path.dirname(__file__),
                                 "fixtures", "ansible", "inventory"),
                    cls.tempdir)


    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(os.path.normpath(os.path.join(cls.tempdir, "../")))

    def setUp(self):
        self.requests_file = os.path.join(self.tempdir, "requests.txt")

        # Launch Flask webserver
        self.webserver = multiprocessing.Process(target=flaskProcess,
                                                 args=(self.requests_file, ))
        self.webserver.daemon = True
        self.webserver.start()
        time.sleep(0.3)

    def tearDown(self):
        os.remove(self.requests_file)
        self.webserver.terminate()
        self.webserver.join()

        pass

    def run_playbook(self, playbook, extra_vars=None):
        env = os.environ.copy()
        env.update({
            'LOG_WRITE_URL': 'http://localhost:5000/log',
            'GIRDER_TOKEN': 'mock_girder_token',
            'CLUSTER_ID': 'mock_cluster_id'})

        _run_playbook(
            os.path.join(self.tempdir, playbook),
            inventory=os.path.join(self.tempdir, "inventory"),
            extra_vars=extra_vars,
            env=env)

        with open(self.requests_file, "rb") as fh:
            contents = fh.read()

        return [json.loads(l) for l in contents.split("\n") if l != '']

    def test_run(self):
        sources = self.run_playbook("test_run_playbook.yml")
        targets = [{"status": "starting",
                    "data": None,
                    "message": "localhost",
                    "type": "play"},
                   {"status": "starting",
                    "data": None,
                    "message": "debug Works!",
                    "type": "task"},
                   {"status": "finished",
                    "data": {"msg": "Hello world!", "verbose_always": True,
                             "host": "localhost", "module_name": "debug"},
                    "message": "debug Works!", "type": "task"},
                   {"status": "finished",
                    "data": None,
                    "message": "localhost",
                    "type": "play"}]

        # Same length
        self.assertEquals(len(sources), len(targets))

        for source, target in zip(sources, targets):

            # target keys are a subset of source
            self.assertTrue(set(target.keys()) <= set(source.keys()))

            # target and source values are equal
            for key in target.keys():
                self.assertEquals(source[key], target[key])
