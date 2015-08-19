from girder.models.model_base import ValidationException
from bson.objectid import ObjectId
from girder.constants import AccessType
from .base import BaseModel
from ..constants import ClusterType
from ..utility.cluster_adapters import get_cluster_adapter


class Cluster(BaseModel):

    def __init__(self):
        super(Cluster, self).__init__()

    def initialize(self):
        self.name = 'clusters'

    def validate(self, cluster):
        if not cluster['name']:
            raise ValidationException('Name must not be empty.', 'name')

        if not cluster['type']:
            raise ValidationException('Type must not be empty.', 'type')

        adapter = get_cluster_adapter(cluster)

        return adapter.validate()

    def _create(self, user, cluster):
        self.setUserAccess(cluster, user=user, level=AccessType.ADMIN)
        group = {
            '_id': ObjectId(self.get_group_id())
        }
        doc = self.setGroupAccess(cluster, group, level=AccessType.ADMIN)

        self.save(doc)

        return doc

    def create_ec2(self, user, config_id, name, template):
        cluster = {
            'name': name,
            'template': template,
            'log': [],
            'status': 'created',
            'config': {
                '_id': config_id
            },
            'type': ClusterType.EC2
        }

        return self._create(user, cluster)

    def create_traditional(self, user, name, hostname, username):
        cluster = {
            'name': name,
            'log': [],
            'status': 'running',
            'config': {
                'hostname': hostname,
                'ssh': {
                    'username': username
                }
            },
            'type': ClusterType.TRADITIONAL
        }

        return self._create(user, cluster)

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
            {'_id': cluster['config']['_id']})

        return self.remove(cluster)
