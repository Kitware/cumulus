import mock
import json
import cherrypy
from tests import base
from cumulus.constants import ClusterStatus


def setUpModule():
    base.enabledPlugins.append('cumulus')
    cherrypy.server.socket_port = 8081
    base.startServer(mock=False)


def tearDownModule():
    base.stopServer()


class AnsibleTestCase(base.TestCase):

    def setUp(self):
        super(AnsibleTestCase, self).setUp()
        users = ({
            'email': 'cumulus@email.com',
            'login': 'cumulus',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        }, {
            'email': 'regularuser@email.com',
            'login': 'regularuser',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        })

        self._cumulus, self._user = \
            [self.model('user').createUser(**user) for user in users]

        self._group = self.model('group').createGroup('cumulus', self._cumulus)

    def test_cluster_status(self):
        names = [s.name for s in ClusterStatus]
        self.assertTrue("error" in names)
        self.assertTrue("creating" in names)
        self.assertTrue("created" in names)
        self.assertTrue("launching" in names)
        self.assertTrue("launched" in names)
        self.assertTrue("provisioning" in names)
        self.assertTrue("provisioned" in names)
        self.assertTrue("terminating" in names)
        self.assertTrue("termindated" in names)
        self.assertTrue("stopped" in names)
        self.assertTrue("running" in names)

        self.assertTrue(ClusterStatus.error < ClusterStatus.creating)
        self.assertTrue(ClusterStatus.creating < ClusterStatus.created)
        self.assertTrue(ClusterStatus.created < ClusterStatus.launching)
        self.assertTrue(ClusterStatus.launching < ClusterStatus.launched)
        self.assertTrue(ClusterStatus.launched < ClusterStatus.provisioning)
        self.assertTrue(ClusterStatus.provisioning < ClusterStatus.provisioned)
        self.assertTrue(ClusterStatus.provisioned < ClusterStatus.terminating)
        self.assertTrue(ClusterStatus.terminating < ClusterStatus.terminated)
        self.assertTrue(ClusterStatus.terminated < ClusterStatus.stopped)
        self.assertTrue(ClusterStatus.stopped < ClusterStatus.running)

    def test_create(self):
        pass
