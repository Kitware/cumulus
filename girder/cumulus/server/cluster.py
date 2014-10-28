import cherrypy
import json

from girder.api.rest import Resource
from girder.api import access
from girder.api.describe import Description



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

        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('cluster', 'cumulus')

    @access.public
    def handle_log_record(self, id, params):
        return self._model.add_log_record(id, json.load(cherrypy.request.body))

    @access.public
    def create(self, params):
        name = params['name']
        template = params['template']

        return self._model.create(name, template)

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
            required=True))

    @access.public
    def start(self, id, params):

        log_write_url = cherrypy.url().replace('start', 'log')
        status_url = cherrypy.url().replace('start', 'status')

        return self._model.start(id, log_write_url, status_url)

    start.description = (Description(
            'Start a cluster'
        )
        .param(
            'id',
            'The cluster id to start.', paramType='path'))

    @access.public
    def update_status(self, id, params):
        status = params['status']

        return self._model.update_status(id, status)

    update_status.description = (Description(
            'Update the clusters current state'
        )
        .param(
            'id',
            'The cluster id to update status on.', paramType='path')
        .param(
            'status',
            'The cluster status.', paramType='query'))

    @access.public
    def status(self, id, params):
        status = self._model.status(id)

        return {'status': status}

    status.description = (Description(
            'Get the clusters current state'
        )
        .param(
            'id',
            'The cluster id to get the status of.', paramType='path'))

    @access.public
    def terminate(self, id, params):

        log_write_url = cherrypy.url().replace('terminate', 'log')
        status_url = cherrypy.url().replace('terminate', 'status')

        return self._model.terminate(id, log_write_url, status_url)

    terminate.description = (Description(
            'Terminate a cluster'
        )
        .param(
            'id',
            'The cluster to terminate.', paramType='path'))

    @access.public
    def log(self, id, params):

        offset = 0
        if 'offset' in params:
            offset = int(params['offset'])

        log_records = self._model.log_records(id, offset)

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

#    @access.public
#    def config(self, id):
#        pass

#    config.description = (Description(
#            'Get StarCluster configuration for this cluster'
#        )
#        .param(
#            'id',
#            'The cluster to get configuration for.', paramType='path'))



