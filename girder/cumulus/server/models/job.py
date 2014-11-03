import cherrypy
from girder.models.model_base import AccessControlledModel
from bson.objectid import ObjectId
from girder.constants import AccessType


class Job(AccessControlledModel):

    def initialize(self):
        self.name = 'jobs'

    def validate(self, doc):
        return doc

    def create(self, user, name,  commands, output_collection_id, on_complete=None):
        job = {
            'name': name,
            'commands': commands,
            'outputCollectionId': output_collection_id,
            'status': 'created',
            'log': []
        }

        if on_complete:
            job['onComplete'] = on_complete

        doc  = self.setUserAccess(job, user=user, level=AccessType.ADMIN, save=True)

        return doc

    def status(self, user, id):
        job = self.load(id, user=user, level=AccessType.READ)

        return job['status']

    def update_job(self, user, id, status=None, sge_id=None):
        # Load first to force access check
        job = self.load(id, user=user, level=AccessType.ADMIN)

        if status:
            job['status'] = status

        if sge_id:
            job['sgeId'] = sge_id

        return self.save(job)

    def add_log_record(self, user, _id, record):
        # Load first to force access check
        print type(id)
        self.load(_id, user=user, level=AccessType.ADMIN)
        print 'ID: |%s|'%  id
        self.update({'_id': ObjectId(_id)}, {'$push': {'log': record}})

    def log_records(self, user, id, offset=0):
        job = self.load(id, user=user, level=AccessType.READ)

        return job['log'][offset:]
