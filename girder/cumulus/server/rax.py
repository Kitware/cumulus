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
import json
from jsonpath_rw import parse

import cumulus
# import cumulus.aws.ec2.tasks.key

from cumulus.common.girder import get_task_token
# from cumulus.aws.ec2 import get_ec2_client

from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType
from girder.api.docs import addModel
from girder.api.rest import RestException, getBodyJson, ModelImporter,\
    getCurrentUser
from girder.api.rest import loadmodel


def requireParams(required, params):
    for param in required:
        if param not in params:
            raise RestException("Parameter '%s' is required." % param)


def _filter(profile):
    del profile['access']
    profile = json.loads(json.dumps(profile, default=str))

    return profile


@access.user
@loadmodel(model='user', level=AccessType.WRITE)
def create_profile(user, params):
    body = getBodyJson()
    requireParams(['userName', 'name', 'apiKey', 'regionName'], body)

    profile_type = 'rax' if 'cloudProvider' not in body.keys() \
                   else body['cloudProvider']

    model = ModelImporter.model('rax', 'cumulus')
    profile = model.create_profile(user['_id'],
                                   profile_type,
                                   body['name'],
                                   body['userName'],
                                   body['regionName'])

    profile['status'] = 'available'
    model.update_rax_profile(user, profile)

#     TODO: Create keypair
#     try:
#         cumulus.aws.ec2.tasks.key.generate_key_pair.delay(
#             _filter(profile), get_task_token()['_id'])
#
#         cherrypy.response.status = 201
#         cherrypy.response.headers['Location'] \
#             = '/user/%s/aws/profile/%s' % (str(user['_id']),
#                                            str(profile['_id']))
#
#         return model.filter(profile, getCurrentUser())
#     except Exception:
#         # Remove profile if error occurs fire of task
#         model.remove(profile)
#         raise


    return model.filter(profile, getCurrentUser())


addModel('RaxParameters', {
    'id': 'RaxParameters',
    'required': ['name', 'accessKeyId', 'secretAccessKey', 'regionName',
                 'regionHost', 'availabilityZone'],
    'properties': {
        'name': {'type': 'string',
                 'description': 'The name of the profile.'},
        'userName':  {'type': 'string',
                      'description': 'The Rackspace Username'},
        'apiKey': {'type': 'string',
                   'description': 'The Rackspace API Key'},
        'regionName': {'type': 'string',
                       'description': 'The Rackspace region'},

    }
}, 'user')

create_profile.description = (
    Description('Create a RAX profile')
    .param('id', 'The id of the user', required=True, paramType='path')
    .param(
        'body',
        'The properties to use to create the profile.',
        dataType='RaxParameters',
        required=True, paramType='body'))


@access.user
@loadmodel(model='user', level=AccessType.READ)
@loadmodel(model='rax', plugin='cumulus',  map={'profileId': 'profile'},
           level=AccessType.WRITE)
def delete_profile(user, profile, params):

    query = {
        'profileId': profile['_id']
    }

    if ModelImporter.model('volume', 'cumulus').findOne(query):
        raise RestException('Unable to delete profile as it is associated with'
                            ' a volume', 400)

    if ModelImporter.model('cluster', 'cumulus').findOne(query):
        raise RestException('Unable to delete profile as it is associated with'
                            ' a cluster', 400)

#     TODO: Delete Key Pair
#     cumulus.aws.ec2.tasks.key.delete_key_pair.delay(_filter(profile),
#                                                     get_task_token()['_id'])
#
#     client = get_ec2_client(profile)
#     client.delete_key_pair(KeyName=str(profile['_id']))

    ModelImporter.model('rax', 'cumulus').remove(profile)


delete_profile.description = (
    Description('Delete a rax profile')
    .param('id', 'The id of the user', required=True, paramType='path')
    .param('profileId', 'The id of the profile to delete', paramType='path',
           required=True))


@access.user
@loadmodel(model='user', level=AccessType.READ)
@loadmodel(model='rax', plugin='cumulus',  map={'profileId': 'profile'},
           level=AccessType.WRITE)
def update_profile(user, profile, params):
    pass
#     body = getBodyJson()
#     properties = ['accessKeyId', 'secretAccessKey', 'status', 'errorMessage',
#                   'publicIPs', 'cloudProvider']
#     for prop in properties:
#         if prop in body:
#             profile[prop] = body[prop]
#
#     ModelImporter.model('aws', 'cumulus').update_aws_profile(getCurrentUser(),
#                                                              profile)
#


addModel('RaxUpdateParameters', {
    'id': 'RaxUpdateParameters',
    'properties': {
        'accessKeyId':  {'type': 'string',
                         'description': 'The aws access key id'},
        'secretAccessKey': {'type': 'string',
                            'description': 'The aws secret access key'},
        'status': {'type': 'string',
                   'description': 'The status of this profile, if its ready'
                   ' to use'},
        'errorMessage': {'type': 'string',
                         'description': 'A error message if a error occured '
                         'during the creation of this profile'}
    }
}, 'user')

update_profile.description = (
    Description('Update a RAX profile')
    .param('id', 'The id of the user', required=True, paramType='path')
    .param('profileId', 'The id of the profile to update', required=True,
           paramType='path')
    .param(
        'body',
        'The properties to use to update the profile.',
        dataType='RaxUpdateParameters',
        required=True, paramType='body'))


@access.user
@loadmodel(model='user', level=AccessType.READ)
def get_profiles(user, params):
    user = getCurrentUser()
    model = ModelImporter.model('rax', 'cumulus')
    limit = params.get('limit', 50)
    profiles = model.find_profiles(user['_id'])

    profiles = model.filterResultsByPermission(profiles, user, AccessType.READ,
                                               limit=int(limit))

    return [model.filter(profile, user) for profile in profiles]


get_profiles.description = (
    Description('Get the RAX profiles for a user')
    .param('id', 'The id of the user', required=True, paramType='path'))


@access.user
@loadmodel(model='user', level=AccessType.READ)
@loadmodel(model='rax', plugin='cumulus',  map={'profileId': 'profile'},
           level=AccessType.WRITE)
def status(user, profile, params):
    return {
        'status': profile['status']
    }


addModel('RaxProfileStatus', {
    'id': 'RaxProfileStatus',
    'required': ['status'],
    'properties': {
        'status': {'type': 'string',
                   'enum': ['creating', 'available', 'error']}
    }
}, 'user')

status.description = (
    Description('Get the status of this profile.')
    .param('id', 'The id of the user', paramType='path')
    .param('profileId', 'The id of the profile to update', required=True,
           paramType='path')
    .responseClass('RaxProfileStatus'))


@access.user
@loadmodel(model='user', level=AccessType.READ)
@loadmodel(model='aws', plugin='cumulus',  map={'profileId': 'profile'},
           level=AccessType.WRITE)
def running_instances(user, profile, params):
    pass
#     return {
#         'runninginstances': get_ec2_client(profile).running_instances()
#     }


addModel('RaxProfileRunningInstances', {
    'id': 'RaxProfileRunningInstances',
    'required': ['runninginstances'],
    'properties': {
        'runninginstances': {'type': 'integer'}
    }
}, 'user')

running_instances.description = (
    Description('Get the number of running instances')
    .param('id', 'The id of the user', paramType='path')
    .param('profileId', 'The id of the profile to update', required=True,
           paramType='path')
    .responseClass('RaxProfileRunningInstances'))


@access.user
@loadmodel(model='user', level=AccessType.READ)
@loadmodel(model='rax', plugin='cumulus',  map={'profileId': 'profile'},
           level=AccessType.WRITE)
def max_instances(user, profile, params):
    pass
#     client = get_ec2_client(profile)
#     response = client.describe_account_attributes(
#         AttributeNames=['max-instances'])
#     jsonpath = 'AccountAttributes[0].AttributeValues[0].AttributeValue'
#     max_instances = parse(jsonpath).find(response)
#
#     if max_instances:
#         max_instances = max_instances[0].value
#     else:
#         raise RestException('Unable to extract "max-instances" attribute.')
#
#     return {
#         'maxinstances': max_instances
#     }


addModel('RaxProfileMaxInstances', {
    'id': 'RaxProfileRunningInstances',
    'required': ['runninginstances'],
    'properties': {
        'maxinstances': {'type': 'integer'}
    }
}, 'user')

max_instances.description = (
    Description('Get the maximum number of instance this account can run')
    .param('id', 'The id of the user', paramType='path')
    .param('profileId', 'The id of the profile to update', required=True,
           paramType='path')
    .responseClass('RaxProfileMaxInstances'))


def load(apiRoot):
    apiRoot.user.route('POST', (':id', 'rax', 'profiles'), create_profile)
    apiRoot.user.route('DELETE', (':id', 'rax', 'profiles', ':profileId'),
                       delete_profile)
    apiRoot.user.route('PATCH', (':id', 'rax', 'profiles', ':profileId'),
                       update_profile)
    apiRoot.user.route('GET', (':id', 'rax', 'profiles'), get_profiles)
    apiRoot.user.route('GET', (':id', 'rax', 'profiles', ':profileId',
                               'status'), status)
    apiRoot.user.route('GET', (':id', 'rax', 'profiles', ':profileId',
                               'runninginstances'), running_instances)
    apiRoot.user.route('GET', (':id', 'rax', 'profiles', ':profileId',
                               'maxinstances'), max_instances)
