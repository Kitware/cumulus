from girder.models.model_base import ValidationException
from bson.objectid import ObjectId
from girder.constants import AccessType
from .base import BaseModel


class Cluster(BaseModel):

    def __init__(self):
        super(Cluster, self).__init__()

    def initialize(self):
        self.name = 'clusters'

    def validate(self, doc):

        if not doc['name']:
            raise ValidationException('Name must not be empty.', 'name')

        query = {
            'name': doc['name']
        }

        if '_id' in doc:
            query['_id'] = {'$ne': doc['_id']}

        duplicate = self.findOne(query, fields=['_id'])
        if duplicate:
            raise ValidationException(
                'A cluster with that name already exists.', 'name')

        # Check the template exists
        config = self.model('starclusterconfig', 'cumulus').load(
            doc['configId'], force=True)
        config = config['config']

        found = False

        if 'cluster' in config:
            for template in config['cluster']:
                name, _ = template.iteritems().next()

                if doc['template'] == name:
                    found = True
                    break

        if not found:
            raise ValidationException(
                'A cluster template \'%s\' not found in configuration.'
                % doc['template'], 'template')

        return doc

    def create(self, user, config_id, name, template):
        cluster = {'name': name, 'template': template,
                   'log': [], 'status': 'created', 'configId': config_id}

        self.setUserAccess(cluster, user=user, level=AccessType.ADMIN)
        group = {
            '_id': ObjectId(self.get_group_id())
        }
        doc = self.setGroupAccess(cluster, group, level=AccessType.ADMIN)

        self.save(doc)

        return doc

    def add_log_record(self, user, id, record):
        # Load first to force access check
        self.load(id, user=user, level=AccessType.WRITE)
        self.update({'_id': ObjectId(id)}, {'$push': {'log': record}})

    def update_cluster(self, user, id, status):
        # Load first to force access check
        cluster = self.load(id, user=user, level=AccessType.WRITE)

        if status:
            cluster['status'] = status

        return self.save(cluster)

    def log_records(self, user, id, offset=0):
        # TODO Need to figure out perms a remove this force
        cluster = self.load(id, user=user, level=AccessType.READ)

        return cluster['log'][offset:]

    def delete(self, user, id):
        cluster = self.load(id, user=user, level=AccessType.ADMIN)

        # Remove the config associated with the cluster first
        self.model('starclusterconfig', 'cumulus').remove(
            {'_id': cluster['configId']})

        return self.remove(cluster)
