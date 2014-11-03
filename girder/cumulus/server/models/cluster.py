import cherrypy
from girder.models.model_base import AccessControlledModel
from bson.objectid import ObjectId
from girder.constants import AccessType


class Cluster(AccessControlledModel):

    def initialize(self):
        self.name = 'clusters'

    def validate(self, doc):
        return doc

    def create(self, user, config_id, name, template):
        cluster = {'name': name, 'template': template,
                   'log': [], 'status': 'created', 'configId': config_id}

        doc  = self.setUserAccess(cluster, user=user, level=AccessType.ADMIN, save=True)

        return doc

    def add_log_record(self, user, id, record):
        # Load first to force access check
        self.load(id, user=user, level=AccessType.ADMIN)
        self.update({'_id': ObjectId(id)}, {'$push': {'log': record}})

    def update_cluster(self, user, id, status):
        # Load first to force access check
        cluster = self.load(id, user=user, level=AccessType.ADMIN)

        if status:
            cluster['status'] = status

        return self.save(cluster)

    def status(self, user, id):
        cluster = self.load(id, user=user, level=AccessType.READ)
        return cluster['status']

    def log_records(self, user, id, offset=0):
        # TODO Need to figure out perms a remove this force
        cluster = self.load(id, user=user, level=AccessType.READ)

        return cluster['log'][offset:]
