import io
import cherrypy
import json
import time
from girder.api.rest import RestException
from girder.api import access
from girder.api.describe import Description
from ConfigParser import ConfigParser
from girder.api.docs import addModel
from girder.constants import AccessType
from girder.api.rest import RestException
from .base import BaseResource
import cumulus
from jinja2 import Template
import requests
import sys
from cumulus.task import runner

class Task(BaseResource):

    DOLLAR_REF = '#dollar-ref#'

    def __init__(self):
        self.resourceName = 'tasks'
        self.route('POST', (), self.create)
        self.route('PUT', (':id', 'run'), self.run)
        self.route('PATCH', (':id',), self.update)
        self.route('GET', (':id','status'), self.status)
        self.route('GET', (':id',), self.get)
        self.route('POST', (':id','log'), self.log)
        self.route('PUT', (':id','terminate'), self.terminate)
        self.route('DELETE', (':id',), self.delete)
        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('task', 'task')

    def _clean(self, task):
        del task['access']

        return task

    def _check_status(self, request):
        if request.status_code != 200:
            print >> sys.stderr, request.content
            request.raise_for_status()

    @access.user
    def create(self, params):
        user = self.getCurrentUser()

        task = json.load(cherrypy.request.body)

        if 'taskSpecId' not in task:
            raise RestException('taskSpecId is required', code=400)

        if not self.model('file').load(task['taskSpecId']):
            raise RestException('Task specification %s doesn\'t exist' % task['taskSpecId'], code=400)

        task['status'] = 'created'
        task = self._model.create(user, task)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/tasks/%s' % task['_id']

        return self._clean(task)

    addModel("TaskIdParam", {
        "id": "TaskIdParam",
        "properties": {
            "taskSpecId": {
                "type": "string"
            }
        }
    })


    create.description = (Description(
        'Create task from a spec file id'
    )
    .param(
        'body',
        'The JSON parameters',
        required=True, paramType='body', dataType='TaskIdParam'))

    @access.user
    def run(self, id, params):
        user = self.getCurrentUser()

        variables = json.load(cherrypy.request.body)

        task = self._model.load(id, user=user)
        task['status'] = 'running'
        task['output'] = {}
        task['log'] = []
        task['onTerminate'] = []
        task['onDelete'] = []
        task['startTime'] = int(round(time.time() * 1000))

        self._model.save(task)

        try:
            file = self.model('file').load(task['taskSpecId'])
            spec = reduce(lambda x, y: x + y, self.model('file').download(file, headers=False)())
            spec = json.loads(spec)

            # Create token for user runnig this task
            token = self.model('token').createToken(user=user, days=7)

            runner.run(token['_id'], self._clean(task), spec, variables)
        except requests.HTTPError as err:
            task['status'] = 'failure'
            self._model.save(task)
            raise RestException(err.response.content, err.response.status_code)

    run.description = (Description(
            'Start the task running'
        )
        .param(
            'id',
            'The id of task',
            required=True, paramType='path')
        .param(
            'variables',
            'The variable to render the task spec with',
            required=False, paramType='body'))

    @access.user
    def update(self, id, params):
        user = self.getCurrentUser()

        body = cherrypy.request.body.read()

        if not body:
            raise RestException('A body must be provided', code=400)

        updates = json.loads(body)

        task = self._model.load(id, user=user, level=AccessType.WRITE)
        if not task:
            raise RestException('Task not found.', code=404)

        if 'status' in updates:
            task['status'] = updates['status']

        if 'output' in updates:
            task['output'] = updates['output']

        if 'onTerminate' in updates:
            for url in updates['onTerminate']:
                task['onTerminate'].insert(0, url)

        if 'onDelete' in updates:
            for url in updates['onDelete']:
                task['onDelete'].insert(0, url)

        if 'endTime' in updates:
            task['endTime'] = updates['endTime']

        self._model.save(task)

        return self._clean(task)

    update.description = (Description(
            'Update the task'
        )
        .param(
            'id',
            'The id of task',
            required=True, paramType='path')
        .param(
            'updates',
            'The properties to update',
            required=False, paramType='body'))

    @access.user
    def status(self, id, params):
        user = self.getCurrentUser()

        task = self._model.load(id, user=user, level=AccessType.READ)
        if not task:
            raise RestException('Task not found.', code=404)

        return {'status': task['status']}

    status.description = (Description(
            'Get the task status'
        )
        .param(
            'id',
            'The id of task',
            required=True, paramType='path'))

    @access.user
    def get(self, id, params):
        user = self.getCurrentUser()

        task = self._model.load(id, user=user, level=AccessType.READ)

        if not task:
            raise RestException('Task not found.', code=404)

        if 'log' in task:
            log = task['log']
            for e in log:
                if Task.DOLLAR_REF in e:
                    e['$ref'] = e[Task.DOLLAR_REF]
                    del e[Task.DOLLAR_REF]

        return self._clean(task)

    get.description = (Description(
            'Get the task '
        )
        .param(
            'id',
            'The id of task',
            required=True, paramType='path'))

    @access.user
    def log(self, id, params):
        user = self.getCurrentUser()

        task = self._model.load(id, user=user, level=AccessType.WRITE)

        if not task:
            raise RestException('Task not found.', code=404)

        body = cherrypy.request.body.read()
        body = body.replace('$ref', Task.DOLLAR_REF)

        if not body:
            raise RestException('Log entry must be provided', code=400)

        task['log'].append(json.loads(body))
        self._model.save(task)

    log.description = None

    @access.user
    def terminate(self, id, params):
        user = self.getCurrentUser()

        task = self._model.load(id, user=user, level=AccessType.WRITE)

        if not task:
            raise RestException('Task not found.', code=404)

        try:
            if 'onTerminate' in task:
                headers = {
                    'Girder-Token': self.get_task_token()['_id']
                }

                success = True
                for terminate_url in task['onTerminate']:
                    try:
                        r = requests.put(terminate_url, headers=headers)
                        self._check_status(r)
                    except requests.HTTPError as ex:
                        entry = {
                            'statusCode': ex.response.status_code,
                            'content': ex.response.content,
                            'stack': traceback.format_exc()
                        }
                        task['log'].append(entry)
                        success = False
                    except Exception as ex:
                        entry = {
                            'msg': ex.message,
                            'stack': traceback.format_exc()
                        }
                        task['log'].append(entry)
                        success = False

            if success:
                task['status'] = 'terminated'
            else:
                task['status'] = 'failure'
                raise RestException('Task termination failed.', code=500)
        finally:
            self._model.save(task)


    terminate.description = (Description(
            'Terminate the task '
        )
        .param(
            'id',
            'The id of task',
            required=True, paramType='path'))

    @access.user
    def delete(self, id, params):
        user = self.getCurrentUser()

        task = self._model.load(id, user=user, level=AccessType.WRITE)

        if not task:
            raise RestException('Task not found.', code=404)

        try:
            if 'onDelete' in task:
                headers = {
                    'Girder-Token': self.get_task_token()['_id']
                }

                for delete_url in task['onDelete']:
                    r = requests.delete(delete_url, headers=headers)
                    self._check_status(r)

            self._model.remove(task)
        except:
            task['state'] = 'failure'
            raise

    delete.description = (Description(
            'Delete the task '
        )
        .param(
            'id',
            'The id of task',
            required=True, paramType='path'))
