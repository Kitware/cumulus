import sys

print >> sys.stderr,  "LOADING >>>>"

from .cluster import Cluster
from .starclusterconfig import StarClusterConfig

def load(info):
    print "LOADING"
    info['apiRoot'].clusters = Cluster()
    print dir(info['apiRoot'])
    info['apiRoot'].starcluster_configs = StarClusterConfig()
