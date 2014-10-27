import sys

print >> sys.stderr,  "LOADING >>>>"

from .cluster import Cluster

def load(info):
    print "LOADING"
    info['apiRoot'].clusters = Cluster()