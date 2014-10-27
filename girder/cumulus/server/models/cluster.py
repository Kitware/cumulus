from girder.models.model_base import AccessControlledModel
from bson.objectid import ObjectId

class Cluster(AccessControlledModel):
    def initialize(self):
        self.name = 'clusters'

    def validate(self, doc):
        return doc

    def create(self, params):
        cluster = {'name': params['name'], 'log': []}
        doc = self.save(cluster)
        return str(doc['_id'])

    def add_log_record(self, id, record):
        self.update({'_id': ObjectId(id)}, {'$push': {'log': record}})


