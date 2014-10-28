import cherrypy
from girder.models.model_base import AccessControlledModel
from bson.objectid import ObjectId



class Cluster(AccessControlledModel):

    def initialize(self):
        self.name = 'clusters'

    def validate(self, doc):
        return doc

    def create(self, config_id, name, template):
        cluster = {'name': name, 'template': template,
                   'log': [], 'status': 'stopped', 'configId': config_id}
        doc = self.save(cluster)

        return str(doc['_id'])

    def add_log_record(self, id, record):
        self.update({'_id': ObjectId(id)}, {'$push': {'log': record}})

    def update_status(self, id, status):
        return self.update({'_id': ObjectId(id)}, {'$set': {'status': status}})

    def status(self, id):
        cluster = self.load(id, force=True)
        return cluster['status']

    def log_records(self, id, offset=0):
        # TODO Need to figure out perms a remove this force
        cluster = self.load(id, force=True)

        return cluster['log'][offset:]
