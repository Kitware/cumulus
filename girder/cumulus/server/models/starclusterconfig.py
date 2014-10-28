import cherrypy

from girder.models.model_base import AccessControlledModel
from bson.objectid import ObjectId


class Starclusterconfig(AccessControlledModel):

    def initialize(self):
        self.name = 'starclusterconfigs'

    def validate(self, doc):
        return doc

    def create(self, name,  config):
        doc = {'name': name, 'config': config}
        doc = self.save(doc)

        return str(doc['_id'])

    def get(self, id):
        return self.load(id, force=True)

