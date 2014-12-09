import cherrypy
import json
from girder.api.rest import Resource
from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType
from girder.api.docs import addModel
from girder.api.rest import RestException
import cumulus
from cumulus.starcluster.tasks.celery import command

class Mesh(Resource):
    def __init__(self):
        self.resourceName = 'meshes'
        self.route('PUT', (':mesh_file_id', 'extract', 'surface'), self.extract)

    def get_task_token(self):
        user = self.model('user').find({'login': cumulus.config.girder.user})

        if user.count() != 1:
            raise Exception('Unable to load user "%s"' % cumulus.config.girder.user)

        user = user.next()

        return self.model('token').createToken(user=user, days=7)

    @access.user
    def extract(self, mesh_file_id, params):
        user = self.getCurrentUser()
        body = json.loads(cherrypy.request.body.read())

        if 'output' not in body :
            raise RestException('output must be provided', code=400)

        if 'itemId' not in  body['output']:
            raise RestException('itemId must be provided', code=400)

        if 'name' not in  body['output']:
            raise RestException('itemId must be provided', code=400)

        item_id = body['output']['itemId']

        file = self.model('file').load(mesh_file_id)

        if not file:
            raise RestException('Unable to load mesh file: %s' % mesh_file_id, code=400)

        item = self.model('item').load(item_id, user=user, level=AccessType.READ)
        if not item:
            raise RestException('Unable to load item: %s' % item_id, code=400)

        girder_token = self.get_task_token()['_id']
        command.send_task('cumulus.moab.tasks.mesh.extract', args=(girder_token, mesh_file_id, body['output']))

    addModel('MeshOutput', {
        'id':'MeshOutput',
        'required': ['itemId', 'name'],
        'properties':{
            'itemId': {
                'pattern': '^[0-9a-fA-F]{24}$',
                'type': 'string'
            },
            'name': {
                'type': 'string'
            }
        }
    })

    addModel('ExtractMeshParams', {
        'id':'ExtractMeshParams',
        'required': ['output'],
        'properties':{
            'output': {
                '$ref': 'MeshOutput'
            }
        }
    })

    extract.description = (Description(
            'Extract surface mesh'
        )
        .param(
            'mesh_file_id',
            'The file id containing the mesh.', dataType='string',
            paramType='path', required=True)
        .param(
            'body',
            'The extract parameters.', dataType='ExtractMeshParams', paramType='body', required=True)
        .notes('If an error occurs during processing a file containing error information ' \
               'be upload instead of the surface mess. The file will name will be of the ' \
               'form <output.name>.error'))
