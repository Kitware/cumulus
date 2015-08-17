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
@loadmodel(model='cluster', level=AccessType.WRITE, plugin='cumulus')
def set_sshkey(cluster, params):
    body = json.loads(cherrypy.request.body.read())

    if 'publickey' not in body:
        raise RestException('publickey must appear in message body', 400)

    key = body['publickey']

    if not key:
        raise RestException('Public key should be in message body', 400)

    if not _validate_key(key):
        raise RestException('Invalid key format', 400)

    ssh = cluster.setdefault('ssh', {})

    ssh['publickey'] = key

    ModelImporter.model('cluster', plugin='cumulus').save(cluster)

set_sshkey.description = (
    Description('Set the public ssh key for a cluster.')
    .param('id', 'The ID of the cluster.', paramType='path')
    .param('body', 'The PUBLIC ssh key', required=True, paramType='body')
    .notes('This endpoint should be treated as internal'))


@access.user
@loadmodel(model='cluster', level=AccessType.READ, plugin='cumulus')
def get_sshkey(cluster, params):
    if 'ssh' not in cluster:
        raise RestException('No such resource', 400)
    ssh = cluster['ssh']

    if 'publickey' not in ssh:
        raise RestException('No such resource', 400)

    return {
        'publickey': ssh['publickey']
    }

get_sshkey.description = (
    Description('Get the public ssh key for access to a cluster.')
    .param('id', 'The ID of the cluster.', paramType='path'))


@access.user
@loadmodel(model='cluster', level=AccessType.WRITE, plugin='cumulus')
def set_passphrase(cluster, params):

    body = json.loads(cherrypy.request.body.read())

    if 'passphrase' not in body:
        raise RestException('passphrase must appear in message body', 400)

    ssh = cluster.setdefault('ssh', {})

    ssh['passphrase'] = body['passphrase']
    ModelImporter.model('cluster', plugin='cumulus').save(cluster)

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
    .param('id', 'The ID of the cluster.', paramType='path')
    .param('body', 'The JSON containing the passphrase', required=True,
           dataType='Passphrase', paramType='body')
    .notes('This endpoint should be treated as internal'))


@access.user
@loadmodel(model='cluster', level=AccessType.READ, plugin='cumulus')
def get_passphrase(cluster, params):

    if 'ssh' not in cluster:
        raise RestException('No such resource', 400)

    ssh = cluster['ssh']

    if 'passphrase' not in ssh:
        raise RestException('No such resource', 400)

    return {
        'passphrase': ssh['passphrase']
    }

get_passphrase.description = (
    Description('Get the passphrase for a cluster key.')
    .param('id', 'The ID of the cluster.', paramType='path')
    .notes('This endpoint should be treated as internal'))


@access.user
@loadmodel(model='cluster', level=AccessType.WRITE, plugin='cumulus')
def set_user(cluster, params):

    body = json.loads(cherrypy.request.body.read())

    if 'user' not in body:
        raise RestException('user must appear in message body', 400)

    ssh = cluster.setdefault('ssh', {})
    ssh['user'] = body['user']

    ModelImporter.model('cluster', plugin='cumulus').save(cluster)

addModel('User', {
    'id': 'User',
    'properties': {
        'users': {
            'type': 'string',
            'description': 'The user'
        }
    }
})

set_user.description = (
    Description('Set the user.')
    .param('id', 'The ID of the cluster.', paramType='path')
    .param('body', 'The JSON containing the user', required=True,
           dataType='User', paramType='body'))


@access.user
@loadmodel(model='cluster', level=AccessType.READ, plugin='cumulus')
def get_user(cluster, params):

    if 'ssh' not in cluster:
        raise RestException('No such resource', 400)

    ssh = cluster['ssh']

    if 'user' not in ssh:
        raise RestException('No such resource', 400)

    return {
        'user': ssh['user']
    }

get_user.description = (
    Description('Get the user for a cluster.')
    .param('id', 'The ID of the cluster.', paramType='path'))
