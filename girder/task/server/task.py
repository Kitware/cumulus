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
        #self.route('DELETE', (':id',), self.delete)
        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('task', 'task')

    def _clean(self, config):
        del config['access']

        return config

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

        runner.run(self.get_task_token(), spec, variables)

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
