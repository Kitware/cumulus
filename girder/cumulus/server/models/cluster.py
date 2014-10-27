import cherrypy
from girder.models.model_base import AccessControlledModel
from bson.objectid import ObjectId
from websim.starcluster.tasks import start_cluster, terminate_cluster


class Cluster(AccessControlledModel):

    def initialize(self):
        self.name = 'clusters'

    def validate(self, doc):
        return doc

    def create(self, name, template):
        cluster = {'name': name, 'template': template,
                   'log': [], 'status': 'stopped'}
        doc = self.save(cluster)

        return str(doc['_id'])

    def add_log_record(self, id, record):
        self.update({'_id': ObjectId(id)}, {'$push': {'log': record}})

    def start(self, id, log_write_url, status_url):

        # TODO Need to figure out perms a remove this force
        cluster = self.load(id, force=True)

        start_cluster.delay(cluster['name'], cluster['template'],
                            log_write_url=log_write_url, status_url=status_url)

    def update_status(self, id, status):
        return self.update({'_id': ObjectId(id)}, {'$set': {'status': status}})

    def status(self, id):
        cluster = self.load(id, force=True)
        return {'status': cluster['status']}

    def terminate(self, id, log_write_url, status_url):

        # TODO Need to figure out perms a remove this force
        cluster = self.load(id, force=True)

        terminate_cluster.delay(cluster['name'],
                            log_write_url=log_write_url, status_url=status_url)
