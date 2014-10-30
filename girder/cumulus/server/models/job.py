import cherrypy
from girder.models.model_base import AccessControlledModel
from bson.objectid import ObjectId
from girder.constants import AccessType


class Job(AccessControlledModel):

    def initialize(self):
        self.name = 'jobs'

    def validate(self, doc):
        return doc

    def create(self, user, name,  commands, output_collection_id):
        job = {
            'name': name,
            'commands': commands,
            'outputCollectionId': output_collection_id,
            'status': 'created'
        }

        doc  = self.setUserAccess(job, user=user, level=AccessType.ADMIN, save=True)

        return doc

    def status(self, user, id):
        job = self.load(id, user=user, level=AccessType.READ)

        return job['status']

    def update(self, user, id, status=None, sge_id=None):
        # Load first to force access check
        job = self.load(id, user=user, level=AccessType.ADMIN)

        set = {}

        if status:
            job['status'] = status

        if sge_id:
            job['sgeId'] = sge_id

        return self.save(job)
