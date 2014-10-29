import cherrypy
from girder.api.rest import Resource
from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType


class Job(Resource):
    def __init__(self):
        self.resourceName = 'jobs'
        self.route('POST', (), self.create)
        self.route('PUT', (':id', 'status'), self.update_status)
        self.route('GET', (':id', 'status'), self.status)
        self.route('PUT', (':id', 'terminate'), self.terminate)
        self.route('PUT', (':id', 'sgeJobId'), self.set_sge_job_id)

        self._model = self.model('job', 'cumulus')

    @access.user
    def create(self, params):
        user = self.getCurrentUser()
        script = cherrypy.request.body.read()
        name = params['name']
        return {'id': self._model.create(user, script)}

    create.description = (Description(
            'Create a new job'
        )
        .param(
            'Human readable identify for job',
            'The commands to run.', required=True, paramType='query')
        .param(
            'script',
            'The commands to run.', required=True, paramType='body'))

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
    def update_status(self, id, params):
        status = params['status']
        user = self.getCurrentUser()

        return self._model.update_status(user, id, status)

    update_status.description = (Description(
            'Update the jobs status'
        )
        .param(
            'id',
            'The job id.', paramType='path')
        .param(
            'status',
            'The status of the job.', required=True, paramType='query'))

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

    @access.user
    def set_sge_job_id(self, id, params):
        user = self.getCurrentUser()
        job_id = params['sgeJobId']

        self._model.set_sge_job_id(user, id, job_id)

    set_sge_job_id.description = (Description(
            'Sets the SGE job ID for this job'
        )
        .param(
            'id',
            'The job id.', required=True, paramType='path')
        .param(
            'sgeJobId',
            'The job id.', required=True, paramType='query'))



