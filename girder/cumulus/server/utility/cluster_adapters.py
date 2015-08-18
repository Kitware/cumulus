from girder.utility.model_importer import ModelImporter
from girder.models.model_base import ValidationException

from ..constants import ClusterType


class AbstractClusterAdapter(ModelImporter):
    """
    This defines the interface to be used by all cluster adapters.
    """
    def __init__(self, cluster):
        self.cluster = cluster

    def validate(self):
        """
        Adapters may implement this if they need to perform any validation
        steps whenever the cluster info is saved to the database. It should
        return the document with any necessary alterations in the success case,
        or throw an exception if validation fails.
        """
        return self.cluster


class Ec2ClusterAdapter(AbstractClusterAdapter):
    def validate(self):
        query = {
            'name': self.cluster['name']
        }

        if '_id' in self.cluster:
            query['_id'] = {'$ne': self.cluster['_id']}

        duplicate = self.model('cluster', 'cumulus').findOne(query,
                                                             fields=['_id'])
        if duplicate:
            raise ValidationException(
                'A cluster with that name already exists.', 'name')

        # Check the template exists
        config = self.model('starclusterconfig', 'cumulus').load(
            self.cluster['configId'], force=True)
        config = config['config']

        found = False

        if 'cluster' in config:
            for template in config['cluster']:
                name, _ = template.iteritems().next()

                if self.cluster['template'] == name:
                    found = True
                    break

        if not found:
            raise ValidationException(
                'A cluster template \'%s\' not found in configuration.'
                % self.cluster['template'], 'template')

        return self.cluster


class TraditionClusterAdapter(AbstractClusterAdapter):
    pass

type_to_adapter = {
    ClusterType.EC2: Ec2ClusterAdapter,
    ClusterType.TRADITIONAL: TraditionClusterAdapter
}


def get_cluster_adapter(cluster):
    global type_to_adapter

    return type_to_adapter[cluster['type']](cluster)
