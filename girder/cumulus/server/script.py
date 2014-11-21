import cherrypy
import json
from girder.api.rest import RestException
from girder.api import access
from girder.api.describe import Description
from girder.api.docs import addModel
from girder.constants import AccessType
from girder.api.rest import RestException
from . import base

class Script(base.BaseResource):

    def __init__(self):
        self.resourceName = 'scripts'
        self.route('POST', (), self.create)
        self.route('GET', (':id',), self.get)
        self.route('PATCH', (':id','import'), self.import_script)
        self.route('DELETE', (':id',), self.delete)
        self._model = self.model('script', 'cumulus')

    def _clean(self, config):
        del config['access']

        return config

    @access.user
    def import_script(self, id, params):
        user = self.getCurrentUser()
        lines = cherrypy.request.body.read().splitlines()

        script = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not script:
            raise RestException('Script doesn\'t exist', code=404)

        script['commands'] = lines
        self._model.save(script)

        return self._clean(script);

    import_script.description = (Description(
        'Import script'
    )
    .param(
        'id',
        'The script to upload lines to',
        required=True, paramType='path')
    .param(
        'body',
        'The contents of the script',
        required=True, paramType='body')
    .consumes('text/plain'))

    @access.user
    def create(self, params):
        user = self.getCurrentUser()

        script = json.load(cherrypy.request.body)

        if 'name' not in script:
            raise RestException('Script name is required', code=400)

        script = self._model.create(user, script)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/scripts/%s' % script['_id']

        return self._clean(script)

    addModel('Script', {
        "id": "Script",
        "required": "global",
        "properties": {
            "name": {
                "type": "string"
            },
            "commands": {
                "type": "array",
                "items": {
                    "type": "string"
                }
            }
        }
    })

    create.description = (Description(
        'Create script'
    )
    .param(
        'body',
        'The JSON contain script parameters',
        required=True, paramType='body', dataType='Script'))

    @access.user
    def get(self, id, params):
        user = self.getCurrentUser()

        script = self._model.load(id, user=user, level=AccessType.READ)

        if not script:
            raise RestException('Script not found', code=404)

        return self._clean(script)

    get.description = (Description(
            'Get script'
        )
        .param(
            'id',
            'The id of the script to get',
            required=True, paramType='path'))

    @access.user
    def delete(self, id, params):
        user = self.getCurrentUser()
        script = self._model.load(id, user=user, level=AccessType.ADMIN)

        self._model.remove(script)

    delete.description = (Description(
            'Delete a script'
        )
        .param(
            'id',
            'The script id.', paramType='path', required=True))
