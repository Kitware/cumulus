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
from jsonpath_rw import parse
from bson.objectid import ObjectId
from botocore.exceptions import ClientError

from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType
from girder.api.docs import addModel
from girder.api.rest import RestException, getCurrentUser, getBodyJson
from girder.models.model_base import ValidationException
from girder.api.rest import loadmodel
from .base import BaseResource

from cumulus.constants import VolumeType
from cumulus.constants import VolumeState
from cumulus.constants import ClusterType
from cumulus.aws.ec2 import get_ec2_client


class Volume(BaseResource):

    def __init__(self):
        super(Volume, self).__init__()
        self.resourceName = 'volumes'
        self.route('POST', (), self.create)
        self.route('GET', (':id', ), self.get)
        self.route('GET', (), self.find)
        self.route('GET', (':id', 'status'), self.get_status)
        self.route('PUT', (':id', 'clusters', ':clusterId', 'attach'),
                   self.attach)
        self.route('PUT', (':id', 'detach'), self.detach)
        self.route('DELETE', (':id', ), self.delete)

        self._model = self.model('volume', 'cumulus')

    def _create_ebs(self, body, zone):
        user = self.getCurrentUser()
        name = body['name']
        size = body['size']
        fs = body.get('fs', None)
        profileId = body['profileId']

        return self.model('volume', 'cumulus').create_ebs(user, profileId, name,
                                                          zone, size, fs)

    @access.user
    def create(self, params):
        body = getBodyJson()
        self.requireParams(['name', 'type', 'size', 'profileId'], body)

        if not VolumeType.is_valid_type(body['type']):
                raise RestException('Invalid volume type.', code=400)

        profile_id = parse('profileId').find(body)
        if not profile_id:
            raise RestException('A profile id must be provided', 400)

        profile_id = profile_id[0].value

        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())
        if not profile:
            raise RestException('Invalid profile', 400)

        client = get_ec2_client(profile)

        if 'zone' in body:
            # Check that the zone is valid
            try:
                zone = body['zone']
                client.describe_availability_zones(
                    ZoneNames=[zone])
            except ClientError as ce:
                code = parse('Error.Code').find(ce.reponse)
                if code:
                    code = code[0].value
                else:
                    raise

                if code == 'InvalidParameterValue':
                    raise ValidationException('Zone does not exist in region',
                                              'zone')
                else:
                    raise

        # Use the zone from the profile
        else:
            zone = profile['availabilityZone']

        volume = self._create_ebs(body, zone)
        vol = client.create_volume(
            Size=body['size'], AvailabilityZone=zone)

        # Now set the EC2 volume id
        volume['ec2']['id'] = vol['VolumeId']
        self.model('volume', 'cumulus').save(volume)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/volumes/%s' % volume['_id']

        return self._model.filter(volume, getCurrentUser())

    addModel('AwsParameter', {
        'id': 'ConfigParameter',
        'required': ['_id'],
        'properties': {
            'profileId': {'type': 'string',
                          'description': 'Id of AWS profile to use'}
        }
    }, 'volumes')

    addModel('VolumeParameters', {
        'id': 'VolumeParameters',
        'required': ['name', 'config', 'type', 'zone', 'size'],
        'properties': {
            'name': {'type': 'string',
                     'description': 'The name to give the cluster.'},
            'aws':  {'type': 'AwsParameter',
                     'description': 'The AWS configuration'},
            'type': {'type': 'string',
                     'description': 'The type of volume to create ( currently '
                     'only esb )'},
            'zone': {'type': 'string',
                     'description': 'The availability region'},
            'size': {'type': 'integer',
                     'description': 'The size of the volume to create'}
        },
    }, 'volumes')

    create.description = (
        Description('Create a volume')
        .param(
            'body',
            'The properties to use to create the volume.',
            dataType='VolumeParameters',
            required=True, paramType='body'))

    @access.user
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.READ)
    def get(self, volume, params):

        return self._model.filter(volume, getCurrentUser())

    get.description = (
        Description('Get a volume')
        .param(
            'id',
            'The volume id.', paramType='path', required=True))

    @access.user
    def find(self, params):
        user = self.getCurrentUser()
        query = {}

        if 'clusterId' in params:
            query['clusterId'] = ObjectId(params['clusterId'])

        limit = params.get('limit', 50)

        volumes = self.model('volume', 'cumulus').find(query=query)
        volumes = list(volumes)

        volumes = self.model('volume', 'cumulus') \
            .filterResultsByPermission(volumes, user, AccessType.ADMIN,
                                       limit=int(limit))

        return [self._model.filter(volume, user) for volume in volumes]

    find.description = (
        Description('Search for volumes')
        .param('limit', 'The max number of volumes to return',
               paramType='query', required=False, default=50))

    @access.user
    @loadmodel(map={'clusterId': 'cluster'}, model='cluster', plugin='cumulus',
               level=AccessType.ADMIN)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def attach(self, volume, cluster, params):
        body = getBodyJson()
        self.requireParams(['path'], body)

        if cluster['type'] != ClusterType.EC2:
            raise RestException('Invalid cluster type', 400)

        profile_id = parse('profileId').find(volume)[0].value
        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())
        volume_id = parse('ec2.id').find(volume)[0].value
        client = get_ec2_client(profile)
        status = self._get_status(client, volume_id)

        if status != VolumeState.AVAILABLE:
            raise RestException('This volume is not available to attach '
                                'to a cluster',
                                400)

        if cluster['status'] == 'running':
            raise RestException('Unable to attach volume to running cluster',
                                400)

        volumes = cluster.setdefault('volumes', [])
        volumes.append(volume['_id'])

        # Add cluster id to volume
        volume['clusterId'] = cluster['_id']

        self.model('cluster', 'cumulus').save(cluster)
        self.model('volume', 'cumulus').save(volume)

    addModel('AttachParameters', {
        'id': 'AttachParameters',
        'required': ['path'],
        'properties': {
            'path': {'type': 'string',
                     'description': 'The path to mount the volume'}
        }
    }, 'volumes')

    attach.description = (
        Description('Attach a volume to a cluster')
        .param(
            'id',
            'The id of the volume to attach', required=True,
            paramType='path')
        .param(
            'clusterId',
            'The cluster to attach the volume to.', required=True,
            paramType='path')
        .param(
            'body',
            'The properties to template on submit.',
            dataType='AttachParameters',
            paramType='body'))

    @access.user
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def detach(self, volume, params):

        if 'clusterId' not in volume:
            raise RestException('Volume is not attached', 400)

        user = getCurrentUser()
        profile_id = parse('profileId').find(volume)[0].value
        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())
        client = get_ec2_client(profile)
        volume_id = parse('ec2.id').find(volume)[0].value
        status = self._get_status(client, volume_id)

        # Call ec2 to do the detach
        if status == VolumeState.INUSE:
            client.detach_volume(VolumeId=volume_id)

        # First remove from cluster
        cluster = self.model('cluster', 'cumulus').load(volume['clusterId'],
                                                        user=user,
                                                        level=AccessType.ADMIN)
        cluster.setdefault('volumes', []).remove(volume['_id'])
        del volume['clusterId']

        self.model('cluster', 'cumulus').save(cluster)
        self.model('volume', 'cumulus').save(volume)

    detach.description = (
        Description('Detach a volume from a cluster')
        .param(
            'id',
            'The id of the attached volume', required=True,
            paramType='path'))

    @access.user
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def delete(self, volume, params):
        if 'clusterId' in volume:
            raise RestException('Unable to delete attached volume')

        # Call EC2 to delete volume
        profile_id = parse('profileId').find(volume)[0].value
        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())

        client = get_ec2_client(profile)
        volume_id = parse('ec2.id').find(volume)[0].value
        client.delete_volume(VolumeId=volume_id)

        self.model('volume', 'cumulus').remove(volume)

    delete.description = (
        Description('Delete a volume')
        .param('id', 'The volume id.', paramType='path', required=True))

    def _get_status(self, client, volume_id):
        response = client.describe_volumes(
            VolumeIds=[volume_id]
        )

        state = parse('Volumes[0].State').find(response)
        if state:
            state = state[0].value
        else:
            raise RestException('Unable to extract volume state from: %s'
                                % state)

        return state

    @access.user
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def get_status(self, volume, params):

        ec2_id = parse('ec2.id').find(volume)[0].value

        if len(ec2_id) < 1:
            return {'status': 'creating'}

        # If we have an ec2 id delegate the call to ec2
        profile_id = parse('profileId').find(volume)[0].value
        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())
        client = get_ec2_client(profile)

        return {'status': self._get_status(client, ec2_id)}

    get_status.description = (
        Description('Get the status of a volume')
        .param('id', 'The volume id.', paramType='path', required=True))
