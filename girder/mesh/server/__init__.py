import sys
from girder.models.model_base import ValidationException
from girder import events
from .mesh import Mesh

def load(info):
    info['apiRoot'].meshes = Mesh()
