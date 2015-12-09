from .rest import Newt

def load(info):
    info['apiRoot'].newt = Newt()
