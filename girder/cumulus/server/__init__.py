from .cluster import Cluster
from .starclusterconfig import StarClusterConfig
from .job import Job
from .script import Script


def load(info):
    info['apiRoot'].clusters = Cluster()
    info['apiRoot'].starcluster_configs = StarClusterConfig()
    info['apiRoot'].jobs = Job()
    info['apiRoot'].scripts = Script()
