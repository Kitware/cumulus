import cherrypy
import json
import re
from jsonpath_rw import parse

from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType
from girder.api.docs import addModel
from girder.api.rest import RestException
from .base import BaseResource
from cumulus.constants import ClusterType
from .utility.cluster_adapters import get_cluster_adapter
import cumulus.starcluster.tasks.job
from cumulus.ssh.tasks.key import generate_key_pair
import cumulus


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

        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('cluster', 'cumulus')

    def _clean(self, cluster):
        del cluster['access']
        del cluster['log']

        user = self.getCurrentUser()
        if parse('config.ssh.passphrase').find(cluster):
            try:
                self.check_group_membership(user, cumulus.config.girder.group)
            except RestException:
                del cluster['config']['ssh']['passphrase']

        cluster['_id'] = str(cluster['_id'])
        if 'config' in cluster and '_id' in cluster['config']:
            cluster['config']['_id'] = str(cluster['config']['_id'])

        return cluster

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

        for c in config:
            if '_id' in c:

                if not c['_id']:
                    raise RestException('Invalid configuration id', 400)

                c = config_model.load(c['_id'], force=True)
                c = c['config']

            loaded_config.append(c)

        config = config_model.create({
            'config': self._merge_configs(loaded_config)
        })

        return config['_id']

    def _create_ec2(self, params, body):

        self.requireParams(['name', 'template', 'config'], body)

        name = body['name']
        template = body['template']
        config = body['config']

        config_id = self._create_config(config)

        user = self.getCurrentUser()

        cluster = self._model.create_ec2(user, config_id, name, template)
        cluster = self._clean(cluster)

        return cluster

    def _create_traditional(self, params, body):

        self.requireParams(['name', 'config'], body)
        self.requireParams(['ssh', 'hostName'], body['config'])
        self.requireParams(['userName'], body['config']['ssh'])

        name = body['name']
        config = body['config']
        user = self.getCurrentUser()
        hostname = config['hostName']
        username = config['ssh']['userName']

        cluster = self._model.create_traditional(user, name, hostname, username)
        cluster = self._clean(cluster)

        # Fire off job to create key pair for cluster
        girder_token = self.get_task_token()['_id']
        generate_key_pair.delay(cluster, girder_token)

        return cluster

    @access.user
    def create(self, params):
        body = json.loads(cherrypy.request.body.read())

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
        cherrypy.response.headers['Location'] = '/cluster/%s' % cluster['_id']

        return cluster

    addModel('Id', {
        "id": "Id",
        "properties": {
            "_id": {"type": "string", "description": "The id."}
        }
    })

    addModel('UserNameParameter', {
        "id": "UserNameParameter",
        "properties": {
            "userName": {"type": "string", "description": "The ssh user id"}
        }
    })

    addModel("SshParameters", {
        "id": "SshParameters",
        "properties": {
            "ssh": {
                "$ref": "UserNameParameter"
            }
        }
    })

    addModel('ClusterParameters', {
        "id": "ClusterParameters",
        "required": ["name", "config", "type"],
        "properties": {
            "name": {"type": "string",
                     "description": "The name to give the cluster."},
            "template":  {"type": "string",
                          "description": "The cluster template to use. "
                          "(ec2 only)"},
            "config": {"type": "array",
                       "description": "List of configuration to use, "
                                      "either ids or inline config.",
                       "items": {"$ref": "Id"}},
            "config": {
                "$ref": "SshParameters",
                "hostName": {"type": "string",
                             "description": "The hostname of the head node "
                                            "(trad only)"}
            },
            "type": {"type": "string",
                     "description": "The cluster type, either 'ec2' or 'trad'"}

        }})

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
        json_body = None

        if cherrypy.request.body:
            request_body = cherrypy.request.body.read()
            if request_body:
                json_body = json.loads(request_body)

        (user, _) = self.getCurrentUser(returnToken=True)
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        cluster = self._clean(cluster)
        adapter = get_cluster_adapter(cluster)
        adapter.start(json_body)

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
    })

    addModel('ClusterStartParams', {
        'id': 'ClusterStartParams',
        'properties': {
            'onStart': {
                '$ref': 'ClusterOnStartParms'
            }
        }
    })

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
        body = json.loads(cherrypy.request.body.read())
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

        cluster = self._model.save(cluster)

        # Now do any updates the adapter provides
        adapter = get_cluster_adapter(cluster)
        adapter.update(body)

        return cluster

    addModel("ClusterUpdateParameters", {
        "id": "ClusterUpdateParameters",
        "properties": {
            "status": {"type": "string", "enum": ["created", "running",
                                                  "stopped", "terminated"],
                       "description": "The new status. (optional)"}
        }
    })

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

    status.description = (Description(
        'Get the clusters current state'
    )
        .param(
            'id',
            'The cluster id to get the status of.', paramType='path'))

    @access.user
    def terminate(self, id, params):
        (user, _) = self.getCurrentUser(returnToken=True)
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        cluster = self._clean(cluster)
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
        (user, token) = self.getCurrentUser(returnToken=True)
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        if cluster['status'] != 'running':
            raise RestException('Cluster is not running', code=400)

        cluster = self._clean(cluster)

        base_url = re.match('(.*)/clusters.*', cherrypy.url()).group(1)

        job_model = self.model('job', 'cumulus')
        job = job_model.load(
            job_id, user=user, level=AccessType.ADMIN)

        # Set the clusterId on the job for termination
        job['clusterId'] = id

        # Add any job parameters to be used when templating job script
        body = cherrypy.request.body.read()
        if body:
            job['params'] = json.loads(body)

        job_model.save(job)

        log_url = '%s/jobs/%s/log' % (base_url, job_id)
        job['_id'] = str(job['_id'])
        del job['access']

        girder_token = self.get_task_token()['_id']
        print "before"
        cumulus.starcluster.tasks.job.submit(girder_token, cluster, job,
                                             log_url)
        print "after"

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

        cluster = self._clean(cluster)

        return cluster

    get.description = (
        Description('Get a cluster')
        .param(
            'id',
            'The cluster is.', paramType='path', required=True))

    @access.user
    def delete(self, id, params):
        user = self.getCurrentUser()

        if not self._model.load(id, user=user, level=AccessType.ADMIN):
            raise RestException('Cluster not found.', code=404)

        self._model.delete(user, id)

    delete.description = (
        Description('Delete a cluster and its configuration')
        .param('id', 'The cluster id.', paramType='path', required=True))
