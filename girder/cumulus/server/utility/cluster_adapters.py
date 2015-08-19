import cherrypy
import re

from girder.utility.model_importer import ModelImporter
from girder.models.model_base import ValidationException
from girder.api.rest import RestException

from ..constants import ClusterType
import cumulus.starcluster.tasks as tasks
import cumulus


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

    def start(self, request_body):
        """
        Adapters may implement this if they support a start operation.
        """
        raise ValidationException(
            'This cluster type does not support a start operation')

    def terminate(self):
        """
        Adapters may implement this if they support a terminate operation.
        """
        raise ValidationException(
            'This cluster type does not support a terminate operation')


class Ec2ClusterAdapter(AbstractClusterAdapter):

    # TODO This should be replaced with a scoped token, plus there is a
    # duplicate method in base.py
    def get_task_token(self):
        user = self.model('user').find({'login': cumulus.config.girder.user})

        if user.count() != 1:
            raise Exception('Unable to load user "%s"' %
                            cumulus.config.girder.user)

        user = user.next()

        return self.model('token').createToken(user=user, days=7)

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
            self.cluster['config']['_id'], force=True)
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

    def start(self, request_body):
        if self.cluster['status'] == 'running':
            raise RestException('Cluster already running.', code=400)

        on_start_submit = None
        if request_body and 'onStart' in request_body and \
           'submitJob' in request_body['onStart']:
            on_start_submit = request_body['onStart']['submitJob']

        base_url = re.match('(.*)/clusters.*', cherrypy.url()).group(1)
        log_write_url = '%s/clusters/%s/log' % (base_url, self.cluster['_id'])
        girder_token = self.get_task_token()['_id']
        tasks.cluster.start_cluster.delay(self.cluster,
                                          log_write_url=log_write_url,
                                          on_start_submit=on_start_submit,
                                          girder_token=girder_token)

    def terminate(self):
        base_url = re.match('(.*)/clusters.*', cherrypy.url()).group(1)
        log_write_url = '%s/clusters/%s/log' % (base_url, self.cluster['_id'])

        if self.cluster['status'] == 'terminated' or \
           self.cluster['status'] == 'terminating':
            return

        girder_token = self.get_task_token()['_id']
        tasks.cluster.terminate_cluster.delay(self.cluster,
                                              log_write_url=log_write_url,
                                              girder_token=girder_token)


class TraditionClusterAdapter(AbstractClusterAdapter):
    pass

type_to_adapter = {
    ClusterType.EC2: Ec2ClusterAdapter,
    ClusterType.TRADITIONAL: TraditionClusterAdapter
}


def get_cluster_adapter(cluster):
    global type_to_adapter

    return type_to_adapter[cluster['type']](cluster)
