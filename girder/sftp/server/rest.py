import requests
import cherrypy

from girder.api.describe import Description
from girder.api.rest import Resource, RestException, getBodyJson, loadmodel
from girder.api import access
from girder.constants import SettingKey
from girder.constants import AssetstoreType, AccessType
from girder.api.docs import addModel

newt_base_url = 'https://newt.nersc.gov/newt'

class SftpAssetstoreResource(Resource):
    def __init__(self):
        super(SftpAssetstoreResource, self).__init__()
        self.resourceName = 'sftp_assetstores'

        self.route('POST', (), self.create_assetstore)
        self.route('POST', (':id', 'files'), self.create_file)

    @access.user
    def create_assetstore(self, params):
        """Create a new SFTP assetstore."""


        self.requireParams(('name', 'host', 'user'), params)

        return self.model('assetstore').save({
            'type': AssetstoreType.SFTP,
            'name': params.get('name'),
            'sftp': {
                'host': params.get('host'),
                'user': params.get('user'),
                'authKey': params.get('authKey')
            }
        })

    create_assetstore.description = (
         Description('Create a new sftp assetstore.')
        .param('name', 'The name of the assetstore', required=True)
        .param('host', 'The host where files are stored', required=True)
        .param('user', 'The user to access the files', required=True)
        .param('authKey', 'A key that can be used to lookup authentication credentials', required=False))


    @access.user
    @loadmodel(model='assetstore')
    def create_file(self, assetstore, params):
        params = getBodyJson()
        self.requireParams(('name', 'itemId', 'size', 'path'), params)
        name = params['name']
        item_id = params['itemId']
        size = params['size']
        path = params['path']
        user = self.getCurrentUser()

        mime_type = params.get('mimeType')
        item = self.model('item').load(id=item_id, user=user,
                                      level=AccessType.WRITE, exc=True)

        file = self.model('file').createFile(
                        name=name, creator=user, item=item, reuseExisting=True,
                        assetstore=assetstore, mimeType=mime_type, size=size)

        file['path'] = path
        file['imported'] = True
        self.model('file').save(file)

        return self.model('file').filter(file)

    addModel('CreateFileParams', {
        'id': 'CreateFileParams',
        'required': ['name', 'itemId', 'size', 'path'],
        'properties': {
            'name': {'type': 'string',
                     'description': 'The name of the file.'},
            'itemId':  {'type': 'string',
                          'description': 'The item to attach the file to.'},
            'size': {'type': 'number',
                       'description': 'The size of the file.'},
            'path': {'type': 'string',
                       'description': 'The full path to the file.'},
            'mimeType': {'type': 'string',
                       'description': 'The the mimeType of the file.'},

            }
        }, 'sftp')

    create_file.description = (
         Description('Create a new file in this assetstore.')
        .param('id', 'The the assetstore to create the file in', required=True,
               paramType='path')
        .param('body', 'The parameter to create the file with.', required=True,
               paramType='body', dataType='CreateFileParams'))





