import cherrypy
import json
from girder.api.rest import Resource
from girder.api import access
from girder.api.describe import Description
from girder.api.docs import addModel
from girder.constants import AccessType


class Job(Resource):
    def __init__(self):
        self.resourceName = 'jobs'
        self.route('POST', (), self.create)
        self.route('PATCH', (':id',), self.update)
        self.route('GET', (':id', 'status'), self.status)
        self.route('PUT', (':id', 'terminate'), self.terminate)

        self._model = self.model('job', 'cumulus')

    @access.user
    def create(self, params):
        user = self.getCurrentUser()

        body = json.loads(cherrypy.request.body.read())
        commands = body['commands']
        name = body['name']
        output_collection_id = body['outputCollectionId']

        job = self._model.create(user, name, commands, output_collection_id)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/jobs/%s' % job['_id']

        # Don't return the access object
        del job['access']

        return job

    addModel('JobParameters', {
        "id":"JobParameters",
        "properties":{
            "commands": {"type": "string", "description": "The commands to run."},
            "name":  {"type": "string", "description": "The human readable job name."},
            "outputCollectionId": {"type": "string", "description": "The id of the collection to upload the output to."}
        }})

    create.description = (Description(
            'Create a new job'
        )
        .param(
            'body',
            'The job parameters in JSON format.', dataType='JobParameters', paramType='body', required=True))

    @access.user
    def terminate(self, id, params):
        pass

    terminate.description = (Description(
            'Terminate a job'
        )
        .param(
            'id',
            'The job id', paramType='path'))

    @access.user
    def update(self, id, params):
        user = self.getCurrentUser()
        body = json.loads(cherrypy.request.body.read())
        status = None
        sge_id = None

        if 'status' in body:
            status = body['status']

        if 'sgeId' in body:
            sge_id = body['sgeId']

        job = self._model.update(user, id, status, sge_id)

        # Don't return the access object
        del job['access']

        return job

    addModel("JobUpdateParameters", {
        "id":"JobUpdateParameters",
        "properties":{
            "status": {"type": "string", "description": "The new status. (optional)"},
            "sgeId": {"type": "integer", "description": "The SGE job id. (optional)"}
        }
    })


    update.description = (Description(
            'Update the job'
        )
        .param('id',
              'The id of the job to update', paramType='path')
        .param(
            'body',
            'The properties to update.', dataType='JobUpdateParameters' , paramType='body'))

    @access.user
    def status(self, id, params):
        user = self.getCurrentUser()

        return {'status': self._model.status(user, id)}

    status.description = (Description(
            'Get the status of a job'
        )
        .param(
            'id',
            'The job id.', paramType='path'))




