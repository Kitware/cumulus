#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2015 Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the "License" );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################

import cherrypy
import cumulus

from girder.api import access
from girder.api.describe import Description, describeRoute
from girder.constants import AccessType, SortDir, TokenScope
from girder.api.docs import addModel
from girder.api.rest import RestException, getBodyJson, loadmodel
from girder.utility.model_importer import ModelImporter
from .base import BaseResource

from cumulus import tasks
from cumulus.constants import JobState


class Job(BaseResource):
    def __init__(self):
        super(Job, self).__init__()
        self.resourceName = 'jobs'
        self.route('POST', (), self.create)
        self.route('PATCH', (':id',), self.update)
        self.route('GET', (':id', 'status'), self.status)
        self.route('PUT', (':id', 'terminate'), self.terminate)
        self.route('POST', (':id', 'log'), self.append_to_log)
        self.route('GET', (':id', 'log'), self.log)
        self.route('GET', (':id', 'output'), self.output)
        self.route('DELETE', (':id', ), self.delete)
        self.route('GET', (':id',), self.get)
        self.route('GET', (), self.find)

        self._model = ModelImporter.model('job', 'cumulus')

    def _clean(self, job):
        del job['access']
        del job['log']
        job['_id'] = str(job['_id'])
        job['userId'] = str(job['userId'])

        return job

    @access.user(scope=TokenScope.DATA_WRITE)
    def create(self, params):
        user = self.getCurrentUser()

        body = getBodyJson()

        self.requireParams(['name'], body)

        if 'commands' not in body and 'scriptId' not in body:
            raise RestException('command or scriptId must be provided',
                                code=400)

        if 'scriptId' in body:
            script = ModelImporter.model('script', 'cumulus').load(body['scriptId'],
                                                          user=user,
                                                          level=AccessType.READ)
            if not script:
                raise RestException('Script not found', 400)

            del body['scriptId']
            body['commands'] = script['commands']

        if 'onTerminate' in body and 'scriptId' in body['onTerminate']:
            script = ModelImporter.model('script', 'cumulus') \
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
                    '$ref': '#/definitions/InputItem'
                }
            },
            'output': {
                'type': 'array',
                'description': 'The output to upload.',
                'items': {
                    '$ref': '#/definitions/OutputItem'
                }
            },
            'onComplete': {
                '$ref': '#/definitions/JobOnCompleteParams'
            }
        }
    }, 'jobs')

    create.description = (
        Description('Create a new job')
        .param(
            'body',
            'The job parameters in JSON format.', dataType='JobParameters',
            paramType='body', required=True))

    @access.user(scope=TokenScope.DATA_WRITE)
    def terminate(self, id, params):
        (user, token) = self.getCurrentUser(returnToken=True)
        job = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not job:
            raise RestException('Job not found.', code=404)

        cluster_model = ModelImporter.model('cluster', 'cumulus')
        cluster = cluster_model.load(job['clusterId'], user=user,
                                     level=AccessType.ADMIN)

        base_url = cumulus.config.girder.baseUrl
        self._model.update_status(user, id, JobState.TERMINATING)

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

    @access.user(scope=TokenScope.DATA_WRITE)
    def update(self, id, params):
        user = self.getCurrentUser()
        body = getBodyJson()

        job = self._model.load(id, user=user, level=AccessType.WRITE)
        if not job:
            raise RestException('Job not found.', code=404)

        if 'status' in body:
            job['status'] = body['status']

        if 'queueJobId' in body:
            job['queueJobId'] = body['queueJobId']

        if 'output' in body:
            job['output'] = body['output']

        if 'timings' in body:
            if 'timings' in job:
                job['timings'].update(body['timings'])
            else:
                job['timings'] = body['timings']

        if 'dir' in body:
            job['dir'] = body['dir']

        if 'metadata' in body:
            job['metadata'] = body['metadata']

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
                '$ref': '#/definitions/JobStatus',
                'description': 'The new status. (optional)'
            },
            'queueJobId': {'type': 'integer',
                           'description': 'The native queue job id. (optional)'},
            'metadata': {'type': 'object',
                         'description': 'Application metadata. (optional)'}
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

    @access.user(scope=TokenScope.DATA_READ)
    def status(self, id, params):
        user = self.getCurrentUser()

        job = self._model.load(id, user=user, level=AccessType.READ)

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

    @access.user(scope=TokenScope.DATA_WRITE)
    def append_to_log(self, id, params):
        user = self.getCurrentUser()

        job = self._model.load(id, user=user, level=AccessType.WRITE)

        if not job:
            raise RestException('Job not found.', code=404)

        body = getBodyJson()

        if not body:
            raise RestException('Log entry must be provided', code=400)

        return self._model.append_to_log(user, id, body)

    append_to_log.description = None

    @access.user(scope=TokenScope.DATA_READ)
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
            'The offset to start getting entries at.', required=False,
            paramType='query'))

    @access.user(scope=TokenScope.DATA_READ)
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

    @access.user(scope=TokenScope.DATA_READ)
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

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='job', plugin='cumulus', level=AccessType.ADMIN)
    @describeRoute(
        Description('Delete a job')
        .param(
            'id',
            'The job id.', paramType='path', required=True)
        .notes('A running job can not be deleted.')
    )
    def delete(self, job, params):
        user = self.getCurrentUser()
        running_state = [
            JobState.RUNNING, JobState.QUEUED, JobState.TERMINATING,
            JobState.UPLOADING]

        if job['status'] in running_state:
            raise RestException('Unable to delete running job.')

        # Clean up any job output
        if 'clusterId' in job:
            cluster_model = ModelImporter.model('cluster', 'cumulus')
            cluster = cluster_model.load(job['clusterId'], user=user,
                                         level=AccessType.READ)

            # Only try to clean up if cluster is still running
            if cluster and cluster['status'] == 'running':
                cluster = cluster_model.filter(cluster, user)
                girder_token = self.get_task_token(cluster)['_id']
                tasks.job.remove_output.delay(cluster, self._clean(job.copy()),
                                              girder_token=girder_token)

        self._model.remove(job)

    @access.user(scope=TokenScope.DATA_READ)
    def find(self, params):
        user = self.getCurrentUser()

        query = {
            'userId': user['_id']
        }

        limit = int(params.get('limit', 0))
        offset = int(params.get('offset', 0))

        jobs = self._model.find(
            query=query, offset=offset, limit=limit,
            sort=[('name', SortDir.ASCENDING)])

        return [self._clean(job) for job in jobs]

    find.description = (
        Description('List all jobs for a given user')
        .param(
            'offset',
            'The offset into the results', paramType='query', required=False)
        .param(
            'limit',
            'Maximum number of jobs to return', paramType='query',
            required=False))
