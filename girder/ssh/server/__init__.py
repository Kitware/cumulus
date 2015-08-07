import cherrypy
import base64
import json

from girder.api import access
from girder.api.describe import Description
from girder.api.rest import loadmodel
from girder.models.model_base import AccessType
from girder.utility.model_importer import ModelImporter
from girder.api.rest import RestException
from girder.api.docs import addModel


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
    body = json.loads(cherrypy.request.body.read())

    if 'publickey' not in body:
        raise RestException('publickey must appear in message body', 400)

    key = body['publickey']

    if not key:
        raise RestException('Public key should be in message body', 400)

    if not _validate_key(key):
        raise RestException('Invalid key format', 400)

    user['sshkey'] = key
    ModelImporter.model('user').save(user)

set_sshkey.description = (
    Description('Set the public ssh key for a user.')
    .param('id', 'The ID of the user.', paramType='path')
    .param('body', 'The PUBLIC ssh key', required=True, paramType='body')
    .notes('This endpoint should be treated and internal'))


@access.user
@loadmodel(model='user', level=AccessType.READ)
def get_sshkey(user, params):
    if 'sshkey' not in user:
        raise RestException('No such resource', 400)

    return {
        'publickey': user['sshkey']
    }

get_sshkey.description = (
    Description('Get the public ssh key for a user.')
    .param('id', 'The ID of the user.', paramType='path'))


@access.user
@loadmodel(model='user', level=AccessType.WRITE)
def set_passphrase(user, params):

    body = json.loads(cherrypy.request.body.read())

    if 'passphrase' not in body:
        raise RestException('passphrase must appear in message body', 400)

    user['passphrase'] = body['passphrase']
    ModelImporter.model('user').save(user)

addModel('Passphrase', {
    'id': 'Passphrase',
    'properties': {
        'passphrase': {
            'type': 'string',
            'description': 'The passphrase'
        }
    }
})

set_passphrase.description = (
    Description('Set the passphrase for a key.')
    .param('id', 'The ID of the user.', paramType='path')
    .param('body', 'The JSON containing the passphrase', required=True,
           dataType='Passphrase', paramType='body')
    .notes('This endpoint should be treated and internal'))


@access.user
@loadmodel(model='user', level=AccessType.READ)
def get_passphrase(user, params):
    if 'passphrase' not in user:
        raise RestException('No such resource', 400)

    return {
        'passphrase': user['passphrase']
    }

get_passphrase.description = (
    Description('Get the passphrase for a user.')
    .param('id', 'The ID of the user.', paramType='path')
    .notes('This endpoint should be treated and internal'))


def load(info):
    info['apiRoot'].user.route('PATCH', (':id', 'ssh', 'publickey'), set_sshkey)
    info['apiRoot'].user.route('PATCH', (':id', 'ssh', 'passphrase'),
                               set_passphrase)
    info['apiRoot'].user.route('GET', (':id', 'ssh', 'publickey'), get_sshkey)
    info['apiRoot'].user.route('GET', (':id', 'ssh', 'passphrase'),
                               get_passphrase)

    ModelImporter.model('user').exposeFields(
        level=AccessType.READ, fields='sshkey')
