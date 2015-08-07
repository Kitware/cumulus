import cherrypy
import base64

from girder.api import access
from girder.api.describe import Description
from girder.api.rest import loadmodel
from girder.models.model_base import AccessType
from girder.utility.model_importer import ModelImporter
from girder.api.rest import RestException


def _validate_key(key):
    try:
        key_type, key_string, _ = key.split()
        data = base64.decodestring(key_string)
        return data[4:11] == key_type
    except Exception:
        return False


@access.user
@loadmodel(model='user', level=AccessType.WRITE)
def set_sshkey(user, params):
    key = cherrypy.request.body.read()

    if not key:
        raise RestException('Public key should be in message body', 400)

    if not _validate_key(key):
        raise RestException('Invalid key format', 400)

    user['sshkey'] = key
    ModelImporter.model('user').save(user)

set_sshkey.description = (
    Description('Set the public ssh key for a user.')
    .param('id', 'The ID of the user.', paramType='path')
    .param('body', 'The PUBLIC ssh key', required=True, paramType='body'))


@access.user
@loadmodel(model='user', level=AccessType.READ)
def get_sshkey(user, params):
    if 'sshkey' not in user:
        raise RestException('No such resource', 400)

    return {
        'key': user['sshkey']
    }


def load(info):
    info['apiRoot'].user.route('PATCH', (':id', 'sshkey'), set_sshkey)
    info['apiRoot'].user.route('GET', (':id', 'sshkey'), get_sshkey)

    ModelImporter.model('user').exposeFields(
        level=AccessType.READ, fields='sshkey')
