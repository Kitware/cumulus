import cherrypy

from girder.models.model_base import AccessControlledModel
from girder.constants import AccessType
from bson.objectid import ObjectId
import cumulus


class Task(AccessControlledModel):

    def initialize(self):
        self.name = 'tasks'

    def validate(self, doc):
        return doc

    def create(self, user, task):

        doc  = self.setUserAccess(task, user, level=AccessType.ADMIN, save=True)
        print doc

        return doc



