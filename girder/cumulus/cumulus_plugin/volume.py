#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2015 Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the 'License' );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an 'AS IS' BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################

import cherrypy
from jsonpath_rw import parse
from bson.objectid import ObjectId

from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType, TokenScope
from girder.api.docs import addModel
from girder.api.rest import RestException, getCurrentUser, getBodyJson
from girder.api.rest import loadmodel
from girder.utility.model_importer import ModelImporter
from .base import BaseResource

import cumulus
from cumulus.constants import VolumeType
from cumulus.constants import VolumeState
from cumulus.common.girder import get_task_token, _get_profile, \
    send_status_notification

import cumulus.ansible.tasks.volume

from cumulus.ansible.tasks.providers import CloudProvider, InstanceState


class Volume(BaseResource):

    def __init__(self):
        super(Volume, self).__init__()
        self.resourceName = 'volumes'
        self.route('POST', (), self.create)
        self.route('GET', (':id', ), self.get)
        self.route('PATCH', (':id', ), self.patch)
        self.route('GET', (), self.find)
        self.route('GET', (':id', 'status'), self.get_status)
        self.route('POST', (':id', 'log'), self.append_to_log)
        self.route('GET', (':id', 'log'), self.log)
        self.route('PUT', (':id', 'clusters', ':clusterId', 'attach'),
                   self.attach)
        self.route('PUT', (':id', 'clusters', ':clusterId',
                           'attach', 'complete'),
                   self.attach_complete)
        self.route('PUT', (':id', 'detach'), self.detach)
        self.route('PUT', (':id', 'detach', 'complete'), self.detach_complete)
        self.route('DELETE', (':id', ), self.delete)
        self.route('PUT', (':id', 'delete', 'complete'), self.delete_complete)

        self._model = ModelImporter.model('volume', 'cumulus')

    def _create_ebs(self, body, zone):
        user = getCurrentUser()
        name = body['name']
        size = body['size']
        fs = body.get('fs', None)
        profileId = body['profileId']

        return self._model.create_ebs(user, profileId, name, zone, size, fs)

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.WRITE)
    def patch(self, volume, params):
        body = getBodyJson()

        if not volume:
            raise RestException('Volume not found.', code=404)

        if 'ec2' in body:
            if 'ec2' not in volume:
                volume['ec2'] = {}
            volume['ec2'].update(body['ec2'])

        mutable = ['status', 'msg', 'path']
        for k in mutable:
            if k in body:
                volume[k] = body[k]

        user = getCurrentUser()
        volume = self._model.update_volume(user, volume)
        return self._model.filter(volume, user)

    patch.description = (
        Description('Patch a volume')
        .param(
            'id',
            'The volume id.', paramType='path', required=True)
        .param(
            'body',
            'The properties to use to create the volume.',
            required=True, paramType='body'))

    @access.user(scope=TokenScope.DATA_WRITE)
    def create(self, params):
        body = getBodyJson()

        self.requireParams(['name', 'type', 'size', 'profileId'], body)

        if not VolumeType.is_valid_type(body['type']):
            raise RestException('Invalid volume type.', code=400)

        profile_id = parse('profileId').find(body)
        if not profile_id:
            raise RestException('A profile id must be provided', 400)

        profile_id = profile_id[0].value

        profile, secret_key = _get_profile(profile_id)

        if not profile:
            raise RestException('Invalid profile', 400)

        if 'zone' in body:
            zone = body['zone']
        else:
            zone = profile['availabilityZone']

        volume = self._create_ebs(body, zone)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/volumes/%s' % volume['_id']

        return self._model.filter(volume, getCurrentUser())

    addModel('VolumeParameters', {
        'id': 'VolumeParameters',
        'required': ['name', 'config', 'type', 'zone', 'size'],
        'properties': {
            'name': {'type': 'string',
                     'description': 'The name to give the cluster.'},
            'profileId': {'type': 'string',
                          'description': 'Id of profile to use'},
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

    @access.user(scope=TokenScope.DATA_READ)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.READ)
    def get(self, volume, params):

        return self._model.filter(volume, getCurrentUser())

    get.description = (
        Description('Get a volume')
        .param(
            'id',
            'The volume id.', paramType='path', required=True))

    @access.user(scope=TokenScope.DATA_READ)
    def find(self, params):
        user = getCurrentUser()
        query = {}

        if 'clusterId' in params:
            query['clusterId'] = ObjectId(params['clusterId'])

        limit = params.get('limit', 50)

        volumes = self._model.find(query=query)
        volumes = list(volumes)

        volumes = self._model \
            .filterResultsByPermission(volumes, user, AccessType.ADMIN,
                                       limit=int(limit))

        return [self._model.filter(volume, user) for volume in volumes]

    find.description = (
        Description('Search for volumes')
        .param('limit', 'The max number of volumes to return',
               paramType='query', required=False, default=50))

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(map={'clusterId': 'cluster'}, model='cluster', plugin='cumulus',
               level=AccessType.ADMIN)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def attach_complete(self, volume, cluster, params):

        user = getCurrentUser()

        path = params.get('path', None)

        # Is path being passed in as apart of the body json?
        if path is None:
            path = getBodyJson().get('path', None)

        if path is not None:
            cluster.setdefault('volumes', [])
            cluster['volumes'].append(volume['_id'])
            cluster['volumes'] = list(set(cluster['volumes']))

            volume['status'] = VolumeState.INUSE
            volume['path'] = path

            # TODO: removing msg should be refactored into
            #       a general purpose 'update_status' function
            #       on the volume model. This way msg only referes
            #       to the current status.
            try:
                del volume['msg']
            except KeyError:
                pass

            # Add cluster id to volume
            volume['clusterId'] = cluster['_id']

            ModelImporter.model('cluster', 'cumulus').save(cluster)
            self._model.update_volume(user, volume)
        else:
            volume['status'] = VolumeState.ERROR
            volume['msg'] = 'Volume path was not communicated on complete'
            self._model.update_volume(user, volume)

    attach_complete.description = None

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(map={'clusterId': 'cluster'}, model='cluster', plugin='cumulus',
               level=AccessType.ADMIN)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def attach(self, volume, cluster, params):
        body = getBodyJson()

        self.requireParams(['path'], body)
        path = body['path']

        profile_id = parse('profileId').find(volume)[0].value
        profile, secret_key = _get_profile(profile_id)

        girder_callback_info = {
            'girder_api_url': cumulus.config.girder.baseUrl,
            'girder_token': get_task_token()['_id']}
        log_write_url = '%s/volumes/%s/log' % (cumulus.config.girder.baseUrl,
                                               volume['_id'])

        p = CloudProvider(dict(secretAccessKey=secret_key, **profile))

        aws_volume = p.get_volume(volume)

        # If volume exists it needs to be available to be attached. If
        # it doesn't exist it will be created as part of the attach
        # playbook.
        if aws_volume is not None and \
           aws_volume['state'] != VolumeState.AVAILABLE:
            raise RestException('This volume is not available to attach '
                                'to a cluster',
                                400)

        master = p.get_master_instance(cluster['_id'])
        if master['state'] != InstanceState.RUNNING:
            raise RestException('Master instance is not running!',
                                400)

        cluster = ModelImporter.model('cluster', 'cumulus').filter(
            cluster, getCurrentUser(), passphrase=False)
        cumulus.ansible.tasks.volume.attach_volume\
            .delay(profile, cluster, master,
                   self._model.filter(volume, getCurrentUser()), path,
                   secret_key, log_write_url, girder_callback_info)

        volume['status'] = VolumeState.ATTACHING
        volume = self._model.update_volume(getCurrentUser(), volume)

        return self._model.filter(volume, getCurrentUser())

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

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def detach(self, volume, params):

        profile_id = parse('profileId').find(volume)[0].value
        profile, secret_key = _get_profile(profile_id)

        girder_callback_info = {
            'girder_api_url': cumulus.config.girder.baseUrl,
            'girder_token': get_task_token()['_id']}

        log_write_url = '%s/volumes/%s/log' % (cumulus.config.girder.baseUrl,
                                               volume['_id'])

        p = CloudProvider(dict(secretAccessKey=secret_key, **profile))

        aws_volume = p.get_volume(volume)
        if aws_volume is None or aws_volume['state'] != VolumeState.INUSE:
            raise RestException('This volume is not attached '
                                'to a cluster',
                                400)

        if 'clusterId' not in volume:
            raise RestException('clusterId is not set on this volume!', 400)

        try:
            volume['path']
        except KeyError:
            raise RestException('path is not set on this volume!', 400)

        cluster = ModelImporter.model('cluster', 'cumulus').load(volume['clusterId'],
                                                        user=getCurrentUser(),
                                                        level=AccessType.ADMIN)
        master = p.get_master_instance(cluster['_id'])
        if master['state'] != InstanceState.RUNNING:
            raise RestException('Master instance is not running!',
                                400)
        user = getCurrentUser()
        cluster = ModelImporter.model('cluster', 'cumulus').filter(
            cluster, user, passphrase=False)
        cumulus.ansible.tasks.volume.detach_volume\
            .delay(profile, cluster, master,
                   self._model.filter(volume, user),
                   secret_key, log_write_url, girder_callback_info)

        volume['status'] = VolumeState.DETACHING
        volume = self._model.update_volume(user, volume)

        return self._model.filter(volume, user)

    detach.description = (
        Description('Detach a volume from a cluster')
        .param(
            'id',
            'The id of the attached volume', required=True,
            paramType='path'))

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def detach_complete(self, volume, params):

        # First remove from cluster
        user = getCurrentUser()
        cluster = ModelImporter.model('cluster', 'cumulus').load(volume['clusterId'],
                                                        user=user,
                                                        level=AccessType.ADMIN)
        cluster.setdefault('volumes', []).remove(volume['_id'])

        del volume['clusterId']

        for attr in ['path', 'msg']:
            try:
                del volume[attr]
            except KeyError:
                pass

        volume['status'] = VolumeState.AVAILABLE

        ModelImporter.model('cluster', 'cumulus').save(cluster)
        self._model.save(volume)
        send_status_notification('volume', volume)

    detach_complete.description = None

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def delete(self, volume, params):
        if 'clusterId' in volume:
            raise RestException('Unable to delete attached volume')

        # If the volume is in state created and it has no ec2 volume id
        # associated with it,  we should be able to just delete it
        if volume['status'] in (VolumeState.CREATED, VolumeState.ERROR):
            if 'id' in volume['ec2'] and volume['ec2']['id'] is not None:
                raise RestException(
                    'Unable to delete volume,  it is '
                    'associated with an ec2 volume %s' % volume['ec2']['id'])

            self._model.remove(volume)
            return None

        log_write_url = '%s/volumes/%s/log' % (cumulus.config.girder.baseUrl,
                                               volume['_id'])

        # Call EC2 to delete volume
        profile_id = parse('profileId').find(volume)[0].value

        profile, secret_key = _get_profile(profile_id)

        girder_callback_info = {
            'girder_api_url': cumulus.config.girder.baseUrl,
            'girder_token': get_task_token()['_id']}

        p = CloudProvider(dict(secretAccessKey=secret_key, **profile))

        aws_volume = p.get_volume(volume)
        if aws_volume['state'] != VolumeState.AVAILABLE:
            raise RestException(
                'Volume must be in an "%s" status to be deleted'
                % VolumeState.AVAILABLE, 400)

        user = getCurrentUser()
        cumulus.ansible.tasks.volume.delete_volume\
            .delay(profile, self._model.filter(volume, user),
                   secret_key, log_write_url, girder_callback_info)

        volume['status'] = VolumeState.DELETING
        volume = self._model.update_volume(user, volume)

        return self._model.filter(volume, user)

    delete.description = (
        Description('Delete a volume')
        .param('id', 'The volume id.', paramType='path', required=True))

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def delete_complete(self, volume, params):
        self._model.remove(volume)

    delete_complete.description = None

    @access.user(scope=TokenScope.DATA_READ)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def get_status(self, volume, params):
        return {'status': volume['status']}

    get_status.description = (
        Description('Get the status of a volume')
        .param('id', 'The volume id.', paramType='path', required=True))

    @access.user(scope=TokenScope.DATA_WRITE)
    def append_to_log(self, id, params):
        user = getCurrentUser()

        if not self._model.load(id, user=user, level=AccessType.ADMIN):
            raise RestException('Volume not found.', code=404)

        return self._model.append_to_log(
            user, id, getBodyJson())

    append_to_log.description = None

    @access.user(scope=TokenScope.DATA_READ)
    def log(self, id, params):
        user = getCurrentUser()
        offset = 0
        if 'offset' in params:
            offset = int(params['offset'])

        if not self._model.load(id, user=user, level=AccessType.READ):
            raise RestException('Volume not found.', code=404)

        log_records = self._model.log_records(user, id, offset)

        return {'log': log_records}

    log.description = (Description(
        'Get log entries for volume'
    )
        .param(
            'id',
            'The volume to get log entries for.', paramType='path')
        .param(
            'offset',
            'The offset to start getting entries at.', required=False,
            paramType='query'))
