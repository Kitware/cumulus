from .cluster import Cluster

def load(info):
    info['apiRoot'].clusters = Cluster()
