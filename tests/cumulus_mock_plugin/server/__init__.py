from .cluster import Cluster
from .starclusterconfig import StarClusterConfig
from .job import Job
from .script import Script


def load(info):
    info['apiRoot'].clusters = Cluster()
