import cherrypy
import re
import base64
from jsonpath_rw import parse

from girder.utility.model_importer import ModelImporter
from girder.models.model_base import ValidationException
from girder.api.rest import RestException

from cumulus.constants import ClusterType
from cumulus.starcluster import tasks
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

    def update(self, request_body):
        """
        Adapters may implement this if they support a update operation.
        """
        raise ValidationException(
            'This cluster type does not support a update operation')

    def delete(self):
        """
        Adapters may implement this if they support a delete operation.
        """
        pass


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

    def update(self, body):
        # Don't return the access object
        del self.cluster['access']
        # Don't return the log
        del self.cluster['log']

        return self.cluster

    def delete(self):
        # Remove the config associated with the cluster first
        self.model('starclusterconfig', 'cumulus').remove(
            {'_id': self.cluster['config']['_id']})


def _validate_key(key):
    try:
        parts = key.split()
        key_type, key_string = parts[:2]
        data = base64.decodestring(key_string)
        return data[4:11] == key_type
    except Exception:
        return False


class TraditionClusterAdapter(AbstractClusterAdapter):
    def update(self, body):

        # Use JSONPath to extract out what we need
        passphrase = parse('config.ssh.passphrase').find(body)
        public_key = parse('config.ssh.publicKey').find(body)

        if passphrase:
            ssh = self.cluster['config'].setdefault('ssh', {})
            ssh['passphrase'] = passphrase[0].value

        if public_key:
            public_key = public_key[0].value
            if not _validate_key(public_key):
                raise RestException('Invalid key format', 400)

            ssh = self.cluster['config'].setdefault('ssh', {})
            ssh['publicKey'] = public_key

        self.cluster = self.model('cluster', 'cumulus').save(self.cluster)

        # Don't return the access object
        del self.cluster['access']
        # Don't return the log
        del self.cluster['log']
        # Don't return the passphrase
        if parse('config.ssh.passphrase').find(self.cluster):
            del self.cluster['config']['ssh']['passphrase']

        return self.cluster

type_to_adapter = {
    ClusterType.EC2: Ec2ClusterAdapter,
    ClusterType.TRADITIONAL: TraditionClusterAdapter
}


def get_cluster_adapter(cluster):
    global type_to_adapter

    return type_to_adapter[cluster['type']](cluster)
