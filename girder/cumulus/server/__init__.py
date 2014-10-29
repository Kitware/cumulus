import sys

print >> sys.stderr,  "LOADING >>>>"

from .cluster import Cluster
from .starclusterconfig import StarClusterConfig
from .job import Job

def load(info):
    info['apiRoot'].clusters = Cluster()
    info['apiRoot'].starcluster_configs = StarClusterConfig()
    info['apiRoot'].jobs = Job()