import cherrypy

from girder.models.model_base import AccessControlledModel
from girder.constants import AccessType
from bson.objectid import ObjectId

class Starclusterconfig(AccessControlledModel):

    def initialize(self):
        self.name = 'starclusterconfigs'

    def validate(self, doc):
        return doc

    def create(self, user, name,  config):
        doc = {'name': name, 'config': config}

        doc  = self.setUserAccess(doc, user=user, level=AccessType.ADMIN, save=True)

        return doc

    def get(self, user, id):
        return self.load(id, user=user, level=AccessType.READ)

