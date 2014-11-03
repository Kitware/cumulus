import cherrypy
import json
import re

from girder.api.rest import Resource
from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType
from girder.api.docs import addModel

from cumulus.starcluster.tasks import start_cluster, terminate_cluster, submit_job


class Cluster(Resource):

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

        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('cluster', 'cumulus')

    def _clean(self, cluster):
        del cluster['access']
        del cluster['log']
        cluster['_id'] = str(cluster['_id'])
        cluster['configId'] = str(cluster['configId'])

        return cluster

    @access.user
    def handle_log_record(self, id, params):
        user = self.getCurrentUser()
        return self._model.add_log_record(user, id, json.load(cherrypy.request.body))

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
                values = dict((k.lower(), v) for k,v in values.iteritems())
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
                        self._merge_sections(sections, merged_config[section_type])
                    else:
                        merged_config[section_type] = sections

        return merged_config

    @access.user
    def _create_config(self, config):
        config_model = self.model('starclusterconfig', 'cumulus')

        loaded_config = []

        for c in config:
            if '_id' in c:
                c = config_model.load(c['_id'],
                        user=self.getCurrentUser(), level=AccessType.ADMIN)
                c = c['config']

            loaded_config.append(c)

        config = config_model.save({'config': self._merge_configs(loaded_config)})

        return config['_id']

    @access.user
    def create(self, params):
        body = json.loads(cherrypy.request.body.read())
        name = body['name']
        template = body['template']
        config = body['config']

        config_id = self._create_config(config)

        user = self.getCurrentUser()

        cluster = self._model.create(user, config_id, name, template)
        cluster = self._clean(cluster)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/cluster/%s' % cluster['_id']

        return cluster

    addModel('Id', {
        "id": "Id",
        "properties": {
            "_id": {"type": "string", "description": "The id."}
        }
    })

    addModel('ClusterParameters', {
        "id": "ClusterParameters",
        "required": ["name", "template", "config"],
        "properties": {
            "name": {"type": "string", "description": "The name to give the cluster."},
            "template":  {"type": "string", "description": "The cluster template to use."},
            "config": {"type": "array", "description": "List of configuration to use, either ids or inline config.",
                       "items": {"$ref": "Id"}}
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
        base_url = re.match('(.*)/clusters.*', cherrypy.url()).group(1)
        log_write_url = '%s/clusters/%s/log' % (base_url, id)
        (user, token) = self.getCurrentUser(returnToken=True)
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)
        cluster = self._clean(cluster)

        print cluster

        start_cluster.delay(cluster, base_url=base_url, log_write_url=log_write_url,
                            girder_token=token['_id'])

    start.description = (Description(
        'Start a cluster'
    )
        .param(
            'id',
            'The cluster id to start.', paramType='path'))

    @access.user
    def update(self, id, params):
        body = json.loads(cherrypy.request.body.read())
        user = self.getCurrentUser()

        if 'status' in body:
            status = body['status']

        cluster = self._model.update_cluster(user, id, status)

        # Don't return the access object
        del cluster['access']
        # Don't return the log
        del cluster['log']

        return cluster

    addModel("ClusterUpdateParameters", {
        "id": "ClusterUpdateParameters",
        "properties": {
            "status": {"type": "string", "description": "The new status. (optional)"}
        }
    })

    update.description = (Description(
        'Update the cluster'
    )
        .param('id',
               'The id of the cluster to update', paramType='path')
        .param(
            'body',
            'The properties to update.', dataType='ClusterUpdateParameters', paramType='body'))

    @access.user
    def status(self, id, params):
        user = self.getCurrentUser()
        status = self._model.status(user, id)

        return {'status': status}

    status.description = (Description(
        'Get the clusters current state'
    )
        .param(
            'id',
            'The cluster id to get the status of.', paramType='path'))

    @access.user
    def terminate(self, id, params):
        base_url = re.match('(.*)/clusters.*', cherrypy.url()).group(1)
        log_write_url = '%s/clusters/%s/log' % (base_url, id)

        (user, token) = self.getCurrentUser(returnToken=True)
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)
        cluster = self._clean(cluster)

        print cluster

        terminate_cluster.delay(cluster, base_url=base_url, log_write_url=log_write_url,
                                girder_token=token['_id'])

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
            'The cluster to get log entries for.', required=False, paramType='query'))

    @access.user
    def submit_job(self, id, jobId, params):
        job_id = jobId
        (user, token) = self.getCurrentUser(returnToken=True)
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)
        cluster = self._clean(cluster)

        base_url = re.match('(.*)/clusters.*', cherrypy.url()).group(1)
        config_url = '%s/starcluster-configs/%s?format=ini' % (
            base_url, cluster['configId'])

        job = self.model('job', 'cumulus').load(
            job_id, user=user, level=AccessType.ADMIN)
        log_url = '%s/jobs/%s/log' % (base_url, job_id)
        job['_id'] = str(job['_id'])
        del job['access']

        submit_job.delay(cluster, job,
                         log_write_url=log_url,  config_url=config_url,
                         girder_token=token['_id'],
                         base_url=base_url)

    submit_job.description = (Description(
        'Submit a job to the cluster'
    )
        .param(
            'id',
            'The cluster to submit the job to.', required=True, paramType='path')
        .param(
            'jobId',
            'The cluster to get log entries for.', required=True, paramType='path'))

    @access.user
    def get(self, id, params):
        user = self.getCurrentUser()
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)
        cluster = self._clean(cluster)

        return cluster

    get.description = (Description(
            'Get a cluster'
        )
        .param(
            'id',
            'The cluster is.', paramType='path', required=True))
