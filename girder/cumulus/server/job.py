import cherrypy

from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType
from girder.api.docs import addModel
from girder.api.rest import RestException, getBodyJson, getApiUrl
from .base import BaseResource

from cumulus.starcluster import tasks


class Job(BaseResource):
    def __init__(self):
        self.resourceName = 'jobs'
        self.route('POST', (), self.create)
        self.route('PATCH', (':id',), self.update)
        self.route('GET', (':id', 'status'), self.status)
        self.route('PUT', (':id', 'terminate'), self.terminate)
        self.route('POST', (':id', 'log'), self.add_log_record)
        self.route('GET', (':id', 'log'), self.log)
        self.route('GET', (':id', 'output'), self.output)
        self.route('DELETE', (':id', ), self.delete)
        self.route('GET', (':id',), self.get)

        self._model = self.model('job', 'cumulus')

    def _clean(self, job):
        del job['access']
        del job['log']
        job['_id'] = str(job['_id'])

        return job

    @access.user
    def create(self, params):
        user = self.getCurrentUser()

        body = getBodyJson()

        if 'commands' not in body and 'scriptId' not in body:
            raise RestException('command or scriptId must be provided',
                                code=400)

        if 'scriptId' in body:
            script = self.model('script', 'cumulus').load(body['scriptId'],
                                                          user=user,
                                                          level=AccessType.READ)
            if not script:
                raise RestException('Script not found', 400)

            del body['scriptId']
            body['commands'] = script['commands']

        if 'onTerminate' in body and 'scriptId' in body['onTerminate']:
            script = self.model('script', 'cumulus') \
                .load(body['onTerminate']['scriptId'], user=user,
                      level=AccessType.READ)
            if not script:
                raise RestException('onTerminate script not found', 400)

            del body['onTerminate']['scriptId']
            body['onTerminate']['commands'] = script['commands']

        if 'input' in body:
            if not isinstance(body['input'], list):
                raise RestException('input must be a list', 400)

            for i in body['input']:
                if i['path'] == body['name']:
                    raise RestException('input can\'t be the same as job name',
                                        400)

        if 'output' in body:
            if not isinstance(body['output'], list):
                raise RestException('output must be a list', 400)

        job = self._model.create(user, body)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/jobs/%s' % job['_id']

        return self._clean(job)

    addModel('JobOnCompleteParams', {
        'id': 'JobOnCompleteParams',
        'properties': {
            'cluster': {
                'type': 'string',
                'enum': ['terminate'],
                'description': 'Cluster operation to perform when job '
                'is complete.'
            }
        }
    }, 'jobs')

    addModel('InputItem', {
        'id': 'InputItem',
        'properties': {
            'itemId': {
                'type': 'string',
                'description': 'The item id'
            },
            'path': {
                'type': 'string',
                'description': 'The path to download this item to'
            }
        }
    }, 'jobs')

    addModel('OutputItem', {
        'id': 'OutputItem',
        'properties': {
            'itemId': {
                'type': 'string',
                'description': 'The item id'
            },
            'path': {
                'type': 'string',
                'description': 'The path to upload, may include * wildcard'
            }
        }
    }, 'jobs')

    addModel('JobParameters', {
        'id': 'JobParameters',
        'required': ['name', 'outputCollectionId'],
        'properties': {
            'commands': {
                'type': 'array',
                'description': 'The commands to run.',
                'items': {
                    'type': 'string'
                }
            },
            'scriptId': {
                'pattern': '^[0-9a-fA-F]{24}$',
                'type': 'string'
            },
            'name': {
                'type': 'string',
                'description': 'The human readable job name.'
            },
            'input': {
                'type': 'array',
                'description': 'Input to the job.',
                'items': {
                    '$ref': 'InputItem'
                }
            },
            'output': {
                'type': 'array',
                'description': 'The output to upload.',
                'items': {
                    '$ref': 'OutputItem'
                }
            },
            'onComplete': {
                '$ref': 'JobOnCompleteParams'
            }
        }
    }, 'jobs')

    create.description = (
        Description('Create a new job')
        .param(
            'body',
            'The job parameters in JSON format.', dataType='JobParameters',
            paramType='body', required=True))

    @access.user
    def terminate(self, id, params):
        (user, token) = self.getCurrentUser(returnToken=True)
        job = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not job:
            raise RestException('Job not found.', code=404)

        cluster = self.model('cluster', 'cumulus') \
            .load(job['clusterId'], user=user, level=AccessType.ADMIN)

        # Clean up cluster ( this should be moving into a utility function )
        del cluster['access']
        del cluster['log']
        cluster['_id'] = str(cluster['_id'])
        cluster['config']['_id'] = str(cluster['config']['_id'])

        base_url = getApiUrl()
        self.model.update_status(id, 'terminating')

        log_url = '%s/jobs/%s/log' % (base_url, id)

        # Clean up job
        job = self._clean(job)

        girder_token = self.get_task_token()['_id']
        tasks.job.terminate_job.delay(cluster, job, log_write_url=log_url,
                                      girder_token=girder_token)

        return job

    terminate.description = (
        Description('Terminate a job')
        .param('id', 'The job id', paramType='path'))

    @access.user
    def update(self, id, params):
        user = self.getCurrentUser()
        body = getBodyJson()

        job = self._model.load(id, user=user, level=AccessType.WRITE)
        if not job:
            raise RestException('Job not found.', code=404)

        if 'status' in body:
            job['status'] = body['status']

        if 'sgeId' in body:
            job['sgeId'] = body['sgeId']

        if 'output' in body:
            job['output'] = body['output']

        if 'timings' in body:
            if 'timings' in job:
                job['timings'].update(body['timings'])
            else:
                job['timings'] = body['timings']

        job = self._model.update_job(user, job)

        # Don't return the access object
        del job['access']
        # Don't return the log
        del job['log']

        return job

    addModel('JobUpdateParameters', {
        'id': 'JobUpdateParameters',
        'properties': {
            'status': {
                'type': 'string',
                'enum': [
                    'created',
                    'queued',
                    'running',
                    'error',
                    'completed'
                ],
                'description': 'The new status. (optional)'
            },
            'sgeId': {'type': 'integer',
                      'description': 'The SGE job id. (optional)'}
        }
    }, 'jobs')

    update.description = (
        Description('Update the job')
        .param('id', 'The id of the job to update', paramType='path')
        .param(
            'body',
            'The properties to update.', dataType='JobUpdateParameters',
            paramType='body')
        .notes('Internal - Used by Celery tasks'))

    @access.user
    def status(self, id, params):
        user = self.getCurrentUser()

        job = self._model.load(id, user=user, level=AccessType.WRITE)

        if not job:
            raise RestException('Job not found.', code=404)

        return {'status': job['status']}

    addModel('JobStatus', {
        'id': 'JobStatus',
        'required': ['status'],
        'properties': {
            'status': {'type': 'string',
                       'enum': ['created', 'downloading', 'queued', 'running',
                                'uploading', 'terminating', 'terminated',
                                'complete', 'error']}
        }
    }, 'jobs')

    status.description = (
        Description('Get the status of a job')
        .param('id', 'The job id.', paramType='path')
        .responseClass('JobStatus'))

    @access.user
    def add_log_record(self, id, params):
        user = self.getCurrentUser()

        job = self._model.load(id, user=user, level=AccessType.WRITE)

        if not job:
            raise RestException('Job not found.', code=404)

        body = getBodyJson()

        if not body:
            raise RestException('Log entry must be provided', code=400)

        job['log'].append(body)
        self._model.save(job)

    add_log_record.description = None

    @access.user
    def log(self, id, params):
        user = self.getCurrentUser()
        offset = 0
        if 'offset' in params:
            offset = int(params['offset'])

        job = self._model.load(id, user=user, level=AccessType.READ)

        if not job:
            raise RestException('Job not found.', code=404)

        return {'log': job['log'][offset:]}

    log.description = (
        Description('Get log entries for job')
        .param(
            'id',
            'The job to get log entries for.', paramType='path')
        .param(
            'offset',
            'The offset to start getting entiries at.', required=False,
            paramType='query'))

    @access.user
    def output(self, id, params):
        user = self.getCurrentUser()

        if 'path' not in params:
            raise RestException('path parameter is required.', code=400)

        path = params['path']

        offset = 0
        if 'offset' in params:
            offset = int(params['offset'])

        job = self._model.load(id, user=user, level=AccessType.READ)

        if not job:
            raise RestException('Job not found.', code=404)

        match = None
        # Find the correct file path
        for output in job['output']:
            if output['path'] == path:
                match = output

        if not match:
            raise RestException('Output path not found', code=404)

        if 'content' not in match:
            match['content'] = []

        return {'content': match['content'][offset:]}

    output.description = (
        Description('Get output entries for job')
        .param(
            'id',
            'The job to get output entries for.', paramType='path')
        .param(
            'path',
            'The path for the output file.', required=True, paramType='query')
        .param(
            'offset',
            'The offset to start getting entries at.', required=False,
            paramType='query'))

    @access.user
    def get(self, id, params):
        user = self.getCurrentUser()
        job = self._model.load(id, user=user, level=AccessType.READ)

        if not job:
            raise RestException('Job not found.', code=404)

        job = self._clean(job)

        return job

    get.description = (
        Description('Get a job')
        .param(
            'id',
            'The job id.', paramType='path', required=True))

    @access.user
    def delete(self, id, params):
        user = self.getCurrentUser()

        job = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not job:
            raise RestException('Job not found.', code=404)

        self._model.remove(job)

    delete.description = (
        Description('Delete a job')
        .param(
            'id',
            'The job id.', paramType='path', required=True))
