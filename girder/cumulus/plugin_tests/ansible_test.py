import mock
import json
import cherrypy
from tests import base
from cumulus.constants import ClusterStatus

from girder.utility.model_importer import ModelImporter

AnsibleClusterAdapter = None

def setUpModule():
    global AnsibleClusterAdapter

    base.enabledPlugins.append('cumulus')
    cherrypy.server.socket_port = 8081
    base.startServer(mock=False)

    from cumulus_plugin.utility.cluster_adapters import AnsibleClusterAdapter

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
            [ModelImporter.model('user').createUser(**user) for user in users]

        self._group = ModelImporter.model('group').createGroup('cumulus', self._cumulus)



    def cluster_dict(self, status):
        return {
            u'_id': "CLUSTER_ID",
            u'config': {
                u'scheduler': {
                    u'type': u'sge'
                },
                u'ssh': {
                    u'user': u'ubuntu',
                    u'key': "PROFILE_ID",
                },
                u'launch': {
                    u'spec': u'default',
                    u'params': {
                        "SOME_PARAM_KEY": "SOME_PARAM_VALUE"
                    }
                }
            },
            u'name': u'test',
            u'profileId': "PROFILE_ID",
            u'status': status,
            u'type': u'ec2',
            u'userId': "USER_ID"
        }

    def test_cluster_status(self):
        self.assertTrue(ClusterStatus.valid(ClusterStatus.CREATED))

        self.assertFalse(ClusterStatus.valid("foo"))



    def test_cluster_status_transition(self):
        self.assertTrue(ClusterStatus.valid_transition(ClusterStatus.CREATED,
                                                       ClusterStatus.LAUNCHING))

        self.assertFalse(ClusterStatus.valid_transition(ClusterStatus.ERROR,
                                                        ClusterStatus.CREATED))

    def test_cluster_status_bad_status(self):
        with self.assertRaises(Exception):
            self.assertTrue(ClusterStatus.valid("foo", ClusterStatus.LAUNCHING))

        with self.assertRaises(Exception):
            self.assertTrue(ClusterStatus.valid(ClusterStatus.LAUNCHING, "foo"))


    def test_cluster_status_object_to(self):

        ca = AnsibleClusterAdapter(self.cluster_dict(ClusterStatus.CREATED))
        ca._state_machine.to(ClusterStatus.LAUNCHING)

        self.assertEquals(ca._state_machine.status, ClusterStatus.LAUNCHING)
        self.assertEquals(ca.status, ClusterStatus.LAUNCHING)
        self.assertEquals(ca.cluster['status'], ClusterStatus.LAUNCHING)

    @mock.patch('cumulus_plugin.models.cluster.Cluster.update_status')
    def test_cluster_status_public_api(self, update_status):
        update_status.return_value = self.cluster_dict(ClusterStatus.LAUNCHING)
        ca = AnsibleClusterAdapter(self.cluster_dict(ClusterStatus.CREATED))
        ca.status = ClusterStatus.LAUNCHING

        self.assertEquals(ca.status, ClusterStatus.LAUNCHING)
        self.assertEquals(ca._state_machine.status, ClusterStatus.LAUNCHING)
        self.assertEquals(ca.cluster['status'], ClusterStatus.LAUNCHING)
