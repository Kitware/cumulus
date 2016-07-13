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
        self.assertTrue(ClusterStatus.validate(ClusterStatus.CREATED,
                                               ClusterStatus.LAUNCHING))

        self.assertFalse(ClusterStatus.validate(ClusterStatus.ERROR,
                                                ClusterStatus.CREATED))

    def test_cluster_status_bad_status(self):
        with self.assertRaises(Exception):
            self.assertTrue(ClusterStatus.validate("foo", ClusterStatus.LAUNCHING))

        with self.assertRaises(Exception):
            self.assertTrue(ClusterStatus.validate(ClusterStatus.LAUNCHING, "foo"))


    def test_create(self):
        pass
