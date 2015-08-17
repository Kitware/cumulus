from girder.utility.model_importer import ModelImporter
from girder.models.model_base import AccessType

from .cluster import Cluster
from .starclusterconfig import StarClusterConfig
from .job import Job
from .script import Script

import ssh


def load(info):
    info['apiRoot'].clusters = Cluster()
    info['apiRoot'].starcluster_configs = StarClusterConfig()
    info['apiRoot'].jobs = Job()
    info['apiRoot'].scripts = Script()

    info['apiRoot'].clusters.route('PATCH', (':id', 'ssh', 'publickey'),
                                   ssh.set_sshkey)
    info['apiRoot'].clusters.route('PATCH', (':id', 'ssh', 'passphrase'),
                                   ssh.set_passphrase)
    info['apiRoot'].clusters.route('GET', (':id', 'ssh', 'publickey'),
                                   ssh.get_sshkey)
    info['apiRoot'].clusters.route('GET', (':id', 'ssh', 'passphrase'),
                                   ssh.get_passphrase)

    ModelImporter.model('cluster',
                        plugin='cumulus').hideFields(level=AccessType.READ,
                                                     fields=['sshkey',
                                                             'passphrase'])
