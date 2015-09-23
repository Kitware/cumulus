import cherrypy
import json
from jsonpath_rw import parse

from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType
from girder.api.docs import addModel
from girder.api.rest import RestException, getBodyJson, getCurrentUser
from girder.api.rest import getApiUrl
from girder.models.model_base import ValidationException
from .base import BaseResource
from cumulus.constants import ClusterType
from .utility.cluster_adapters import get_cluster_adapter
import cumulus.starcluster.tasks.job
from cumulus.ssh.tasks.key import generate_key_pair


class Cluster(BaseResource):

    def __init__(self):
        self.resourceName = 'clusters'
        self.route('POST', (), self.create)
        self.route('POST', (':id', 'log'), self.handle_log_record)
        self.route('GET', (':id', 'log'), self.log)
        self.route('PUT', (':id', 'start'), self.start)
        self.route('PATCH', (':id',), self.update)
        self.route('GET', (':id', 'status'), self.status)
        self.route('PUT', (':id', 'terminate'), self.terminate)
        self.route('PUT', (':id', 'job', ':jobId', 'submit'), self.submit_job)
        self.route('GET', (':id', ), self.get)
        self.route('DELETE', (':id', ), self.delete)
        self.route('GET', (), self.find)

        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('cluster', 'cumulus')

    @access.user
    def handle_log_record(self, id, params):
        user = self.getCurrentUser()

        if not self._model.load(id, user=user, level=AccessType.ADMIN):
            raise RestException('Cluster not found.', code=404)

        return self._model.add_log_record(user, id,
                                          json.load(cherrypy.request.body))

    def _find_section(self, name_to_find, sections):
        for section in sections:
            (name, values) = section.iteritems().next()
            if name == name_to_find:
                return values

        return None

    def _merge_sections(self, sections1, sections2):
        for section in sections1:
            (name, values) = section.iteritems().next()
            matching_section = self._find_section(name, sections2)
            if matching_section:
                values = dict((k.lower(), v) for k, v in values.iteritems())
                matching_section.update(values)
            else:
                sections2.append(section)

    def _merge_configs(self, configs):
        merged_config = None
        for c in reversed(configs):
            if not merged_config:
                merged_config = c
            else:
                for (section_type, sections) in c.iteritems():
                    if section_type in merged_config:
                        self._merge_sections(sections,
                                             merged_config[section_type])
                    else:
                        merged_config[section_type] = sections

        return merged_config

    def _create_config(self, config):
        config_model = self.model('starclusterconfig', 'cumulus')

        loaded_config = []

        if not isinstance(config, list):
            config = [config]

        profile_id = None
        for c in config:
            if not profile_id:
                profile_id = parse('aws.profileId').find(c)
                if profile_id:
                    profile_id = profile_id[0].value
                    # Check this a valid profile
                    profile = \
                        self.model('aws', 'cumulus').load(profile_id,
                                                          user=getCurrentUser())

                    if not profile:
                        raise ValidationException('Invalid profile id')

            if '_id' in c:

                if not c['_id']:
                    raise RestException('Invalid configuration id', 400)

                c = config_model.load(c['_id'], force=True)
                c = c['config']

            loaded_config.append(c)

        doc = {
            'config': self._merge_configs(loaded_config)
        }

        if profile_id:
            doc['aws'] = {
                'profileId': profile_id
            }

        config = config_model.create(doc)

        return config['_id']

    def _create_ec2(self, params, body):

        self.requireParams(['name', 'template', 'config'], body)

        name = body['name']
        template = body['template']
        config = body['config']

        config_id = self._create_config(config)

        user = self.getCurrentUser()

        cluster = self._model.create_ec2(user, config_id, name, template)
        cluster = self._model.filter(cluster, user)

        return cluster

    def _create_traditional(self, params, body):

        self.requireParams(['name', 'config'], body)
        self.requireParams(['ssh', 'host'], body['config'])
        self.requireParams(['user'], body['config']['ssh'])

        name = body['name']
        config = body['config']
        user = self.getCurrentUser()
        hostname = config['host']
        username = config['ssh']['user']

        cluster = self._model.create_traditional(user, name, hostname, username)
        cluster = self._model.filter(cluster, user)

        # Fire off job to create key pair for cluster
        girder_token = self.get_task_token()['_id']
        generate_key_pair.delay(cluster, girder_token)

        return cluster

    @access.user
    def create(self, params):
        body = getBodyJson()

        # Default ec2 cluster
        cluster_type = 'ec2'

        if 'type' in body:
            if not ClusterType.is_valid_type(body['type']):
                raise RestException('Invalid cluster type.', code=400)
            cluster_type = body['type']

        if cluster_type == ClusterType.EC2:
            cluster = self._create_ec2(params, body)
        elif cluster_type == ClusterType.TRADITIONAL:
            cluster = self._create_traditional(params, body)
        else:
            raise RestException('Invalid cluster type.', code=400)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/clusters/%s' % cluster['_id']

        return cluster

    addModel('Id', {
        'id': 'Id',
        'properties': {
            '_id': {'type': 'string', 'description': 'The id.'}
        }
    }, 'clusters')

    addModel('UserNameParameter', {
        'id': 'UserNameParameter',
        'properties': {
            'user': {'type': 'string', 'description': 'The ssh user id'}
        }
    }, 'clusters')

    addModel('SshParameters', {
        'id': 'SshParameters',
        'properties': {
            'ssh': {
                '$ref': 'UserNameParameter'
            }
        }
    }, 'clusters')

    addModel('ClusterParameters', {
        'id': 'ClusterParameters',
        'required': ['name', 'config', 'type'],
        'properties': {
            'name': {'type': 'string',
                     'description': 'The name to give the cluster.'},
            'template':  {'type': 'string',
                          'description': 'The cluster template to use. '
                          '(ec2 only)'},
            'config': {'type': 'array',
                       'description': 'List of configuration to use, '
                                      'either ids or inline config.',
                       'items': {'$ref': 'Id'}},
            'config': {
                '$ref': 'SshParameters',
                'host': {'type': 'string',
                         'description': 'The hostname of the head node '
                                        '(trad only)'}
            },
            'type': {'type': 'string',
                     'description': 'The cluster type, either "ec2" or "trad"'}

        }}, 'clusters')

    create.description = (Description(
        'Create a cluster'
    )
        .param(
            'body',
            'The name to give the cluster.',
            dataType='ClusterParameters',
            required=True, paramType='body'))

    @access.user
    def start(self, id, params):
        body = None

        if cherrypy.request.body:
            request_body = cherrypy.request.body.read().decode('utf8')
            if request_body:
                body = json.loads(request_body)

        user = self.getCurrentUser()
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        cluster = self._model.filter(cluster, user, passphrase=False)
        adapter = get_cluster_adapter(cluster)
        adapter.start(body)

    addModel('ClusterOnStartParms', {
        'id': 'ClusterOnStartParms',
        'properties': {
            'submitJob': {
                'pattern': '^[0-9a-fA-F]{24}$',
                'type': 'string',
                'description': 'The id of a Job to submit when the cluster '
                'is started.'
            }
        }
    }, 'clusters')

    addModel('ClusterStartParams', {
        'id': 'ClusterStartParams',
        'properties': {
            'onStart': {
                '$ref': 'ClusterOnStartParms'
            }
        }
    }, 'clusters')

    start.description = (Description(
        'Start a cluster (ec2 only)'
    )
        .param(
            'id',
            'The cluster id to start.', paramType='path', required=True
        )
        .param(
            'body', 'Parameter used when starting cluster', paramType='body',
            dataType='ClusterStartParams', required=False))

    @access.user
    def update(self, id, params):
        body = getBodyJson()
        user = self.getCurrentUser()

        cluster = self._model.load(id, user=user, level=AccessType.WRITE)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        if 'status' in body:
            cluster['status'] = body['status']

        if 'timings' in body:
            if 'timings' in cluster:
                cluster['timings'].update(body['timings'])
            else:
                cluster['timings'] = body['timings']

        cluster = self._model.update_cluster(user, cluster)

        # Now do any updates the adapter provides
        adapter = get_cluster_adapter(cluster)
        adapter.update(body)

        return self._model.filter(cluster, user)

    addModel('ClusterUpdateParameters', {
        'id': 'ClusterUpdateParameters',
        'properties': {
            'status': {'type': 'string', 'enum': ['created', 'running',
                                                  'stopped', 'terminated'],
                       'description': 'The new status. (optional)'}
        }
    }, 'clusters')

    update.description = (Description(
        'Update the cluster'
    )
        .param('id',
               'The id of the cluster to update', paramType='path')
        .param(
            'body',
            'The properties to update.', dataType='ClusterUpdateParameters',
            paramType='body')
        .notes('Internal - Used by Celery tasks'))

    @access.user
    def status(self, id, params):
        user = self.getCurrentUser()
        cluster = self._model.load(id, user=user, level=AccessType.READ)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        return {'status': cluster['status']}

    addModel('ClusterStatus', {
        'id': 'ClusterStatus',
        'required': ['status'],
        'properties': {
            'status': {'type': 'string',
                       'enum': ['created', 'initializing', 'running',
                                'terminating', 'terminated', 'error']}
        }
    }, 'clusters')

    status.description = (
        Description('Get the clusters current state')
        .param('id',
               'The cluster id to get the status of.', paramType='path')
        .responseClass('ClusterStatus'))

    @access.user
    def terminate(self, id, params):
        user = self.getCurrentUser()
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        cluster = self._model.filter(cluster, user, passphrase=False)
        adapter = get_cluster_adapter(cluster)
        adapter.terminate()

    terminate.description = (Description(
        'Terminate a cluster'
    )
        .param(
            'id',
            'The cluster to terminate.', paramType='path'))

    @access.user
    def log(self, id, params):
        user = self.getCurrentUser()
        offset = 0
        if 'offset' in params:
            offset = int(params['offset'])

        if not self._model.load(id, user=user, level=AccessType.READ):
            raise RestException('Cluster not found.', code=404)

        log_records = self._model.log_records(user, id, offset)

        return {'log': log_records}

    log.description = (Description(
        'Get log entries for cluster'
    )
        .param(
            'id',
            'The cluster to get log entries for.', paramType='path')
        .param(
            'offset',
            'The cluster to get log entries for.', required=False,
            paramType='query'))

    @access.user
    def submit_job(self, id, jobId, params):
        job_id = jobId
        user = self.getCurrentUser()
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        if cluster['status'] != 'running':
            raise RestException('Cluster is not running', code=400)

        cluster = self._model.filter(cluster, user, passphrase=False)

        base_url = getApiUrl()
        job_model = self.model('job', 'cumulus')
        job = job_model.load(
            job_id, user=user, level=AccessType.ADMIN)

        # Set the clusterId on the job for termination
        job['clusterId'] = id

        # Add any job parameters to be used when templating job script
        body = cherrypy.request.body.read().decode('utf8')
        if body:
            job['params'] = json.loads(body)

        job_model.save(job)

        log_url = '%s/jobs/%s/log' % (base_url, job_id)
        job['_id'] = str(job['_id'])
        del job['access']

        girder_token = self.get_task_token()['_id']
        cumulus.starcluster.tasks.job.submit(girder_token, cluster, job,
                                             log_url)

    submit_job.description = (
        Description('Submit a job to the cluster')
        .param(
            'id',
            'The cluster to submit the job to.', required=True,
            paramType='path')
        .param(
            'jobId',
            'The cluster to get log entries for.', required=True,
            paramType='path')
        .param(
            'body',
            'The properties to template on submit.', dataType='object',
            paramType='body'))

    @access.user
    def get(self, id, params):
        user = self.getCurrentUser()
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        return self._model.filter(cluster, user)

    get.description = (
        Description('Get a cluster')
        .param(
            'id',
            'The cluster id.', paramType='path', required=True))

    @access.user
    def delete(self, id, params):
        user = self.getCurrentUser()

        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)
        if not cluster:
            raise RestException('Cluster not found.', code=404)

        cluster = self._model.filter(cluster, user)
        adapter = get_cluster_adapter(cluster)
        adapter.delete()

        self._model.delete(user, id)

    delete.description = (
        Description('Delete a cluster and its configuration')
        .param('id', 'The cluster id.', paramType='path', required=True))

    @access.user
    def find(self, params):
        user = self.getCurrentUser()
        query = {}

        if 'type' in params:
            query['type'] = params['type']

        limit = params.get('limit', 50)

        clusters = self._model.find(query=query)

        clusters = self._model.filterResultsByPermission(clusters, user,
                                                         AccessType.ADMIN,
                                                         limit=int(limit))

        return [self._model.filter(cluster, user) for cluster in clusters]

    find.description = (
        Description('Search for clusters with certain properties')
        .param('type', 'The cluster type to search for', paramType='query',
               required=False)
        .param('limit', 'The max number of clusters to return',
               paramType='query', required=False, default=50))
