import io
import cherrypy
import json
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

    def __init__(self):
        self.resourceName = 'tasks'
        self.route('POST', (), self.create)
        self.route('PUT', (':id', 'run'), self.run)
        self.route('PATCH', (':id',), self.update)
        #self.route('DELETE', (':id',), self.delete)
        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('task', 'task')

    def _clean(self, task):
        del task['access']

        return task

    @access.user
    def create(self, params):
        user = self.getCurrentUser()

        task = json.load(cherrypy.request.body)
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

        file = self.model('file').load(task['taskSpecId'])
        spec = reduce(lambda x, y: x + y, self.model('file').download(file, headers=False)())
        spec = json.loads(spec)

        runner.run(self.get_task_token()['_id'], self._clean(task), spec, variables)

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

        updates = json.load(cherrypy.request.body)

        task = self._model.load(id, user=user)

        if 'status' in updates:
            task['status'] = updates['status']

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
