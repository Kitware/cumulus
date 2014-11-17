import cherrypy
from girder.models.model_base import AccessControlledModel, ValidationException
from bson.objectid import ObjectId
from girder.constants import AccessType
import cumulus
from .base import BaseModel

class Job(BaseModel):

    def __init__(self):
        super(Job, self).__init__()

    def initialize(self):
        self.name = 'jobs'

    def validate(self, doc):
        if not doc['name']:
         raise ValidationException('Name must not be empty.', 'name')

        return doc

    def create(self, user, job):

        job['status'] = 'created'
        job['log'] =  []

        self.setUserAccess(job, user=user, level=AccessType.ADMIN)
        group = {
            '_id': ObjectId(self.get_group_id())
        }
        doc  = self.setGroupAccess(job, group, level=AccessType.ADMIN)

        self.save(job)

        return doc

    def status(self, user, id):
        job = self.load(id, user=user, level=AccessType.READ)

        return job['status']

    def update_job(self, user, id, status=None, sge_id=None):
        # Load first to force access check
        job = self.load(id, user=user, level=AccessType.WRITE)

        if status:
            job['status'] = status

        if sge_id:
            job['sgeId'] = sge_id

        return self.save(job)

    def add_log_record(self, user, _id, record):
        # Load first to force access check
        self.load(_id, user=user, level=AccessType.WRITE)
        self.update({'_id': ObjectId(_id)}, {'$push': {'log': record}})

    def log_records(self, user, id, offset=0):
        job = self.load(id, user=user, level=AccessType.READ)

        return job['log'][offset:]
