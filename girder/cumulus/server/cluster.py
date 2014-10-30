import cherrypy
import json
import re

from girder.api.rest import Resource
from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType

from cumulus.starcluster.tasks import start_cluster, terminate_cluster, submit_job


class Cluster(Resource):

    def __init__(self):
        self.resourceName = 'clusters'
        self.route('POST', (), self.create)
        self.route('POST', (':id', 'log'), self.handle_log_record)
        self.route('GET', (':id', 'log'), self.log)
        self.route('PUT', (':id', 'start'), self.start)
        self.route('PUT', (':id', 'status'), self.update_status)
        self.route('GET', (':id', 'status'), self.status)
        self.route('PUT', (':id', 'terminate'), self.terminate)
        self.route('PUT', (':id', 'job', ':jobId', 'submit'), self.submit_job)

        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('cluster', 'cumulus')

    @access.user
    def handle_log_record(self, id, params):
        user = self.getCurrentUser()
        return self._model.add_log_record(user, id, json.load(cherrypy.request.body))

    @access.user
    def create(self, params):
        name = params['name']
        template = params['template']
        config_id = params['configId']

        user = self.getCurrentUser()

        return {'id': self._model.create(user, config_id, name, template)}

    create.description = (Description(
            'Create a cluster'
        )
        .param(
            'name',
            'The name to give the cluster.',
            required=True, paramType='query')
        .param(
            'template',
            'The cluster template to use'+
            '(default="(empty)")',
            required=True, paramType='query')
        .param(
            'configId',
            'The starcluster config to use when creating',
            required=True, paramType='query'))

    @access.user
    def start(self, id, params):

        log_write_url = cherrypy.url().replace('start', 'log')
        status_url = cherrypy.url().replace('start', 'status')
        status_url = cherrypy.url().replace('start', 'status')
        config_url = re.match('(.*)/clusters.*', cherrypy.url()).group(1)
        config_url += '/starcluster-configs/%s?format=ini'

        (user, token) = self.getCurrentUser(returnToken=True)
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        # Need to replace config id
        config_url = config_url % cluster['configId']

        start_cluster.delay(cluster['name'], cluster['template'],
                            log_write_url=log_write_url, status_url=status_url,
                            config_url=config_url, girder_token=token['_id'])

    start.description = (Description(
            'Start a cluster'
        )
        .param(
            'id',
            'The cluster id to start.', paramType='path'))

    @access.user
    def update_status(self, id, params):
        status = params['status']
        user = self.getCurrentUser()

        return self._model.update_status(user, id, status)

    update_status.description = (Description(
            'Update the clusters current state'
        )
        .param(
            'id',
            'The cluster id to update status on.', paramType='path')
        .param(
            'status',
            'The cluster status.', paramType='query'))

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

        log_write_url = cherrypy.url().replace('terminate', 'log')
        status_url = cherrypy.url().replace('terminate', 'status')
        config_url = re.match('(.*)/clusters.*', cherrypy.url()).group(1)
        config_url += '/starcluster-configs/%s?format=ini'

        (user, token) = self.getCurrentUser(returnToken=True)
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        config_url = config_url % cluster['configId']

        terminate_cluster.delay(cluster['name'],
                            log_write_url=log_write_url, status_url=status_url,
                            config_url=config_url, girder_token=token['_id'])

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
        (user, token) = self.getCurrentUser(returnToken=True)
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        base_url = re.match('(.*)/clusters.*', cherrypy.url()).group(1)
        log_write_url = '%s/clusters/%s/log' % (base_url, id)
        config_url = '%s/starcluster-configs/%s?format=ini' % (base_url, cluster['configId'])

        job = self.model('job', 'cumulus').load(jobId, user=user, level=AccessType.ADMIN)
        job_id = job['_id']
        status_url = '%s/jobs/%s/status' % (base_url, job_id)
        jobid_url = '%s/jobs/%s/sgeJobId' % (base_url, job_id)

        submit_job.delay(cluster['name'], str(job['_id']), job['script'],
                            log_write_url=log_write_url, status_url=status_url,
                            config_url=config_url, girder_token=token['_id'],
                            jobid_url=jobid_url)

    submit_job.description = (Description(
            'Submit a job to the cluster'
        )
        .param(
            'id',
            'The cluster to submit the job to.', paramType='path')
        .param(
            'jobId',
            'The cluster to get log entries for.', required=False, paramType='path'))
#    @access.public
#    def config(self, id):
#        pass

#    config.description = (Description(
#            'Get StarCluster configuration for this cluster'
#        )
#        .param(
#            'id',
#            'The cluster to get configuration for.', paramType='path'))



