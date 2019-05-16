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

from bson.objectid import ObjectId
from jsonpath_rw import parse

from girder.models.model_base import ValidationException
from girder.constants import AccessType
from girder.api.rest import getCurrentUser
from girder.utility.model_importer import ModelImporter
from .base import BaseModel
from ..utility.volume_adapters import get_volume_adapter
from cumulus.constants import VolumeType
from cumulus.constants import VolumeState
from cumulus.common.girder import send_log_notification, \
    send_status_notification


class Volume(BaseModel):

    def __init__(self):
        super(Volume, self).__init__()

    def initialize(self):
        self.name = 'volumes'

        self.exposeFields(level=AccessType.READ,
                          fields=('_id', 'config', 'ec2', 'fs', 'name', 'size',
                                  'type', 'zone', 'profileId', 'clusterId',
                                  'status', 'path'))

    def validate(self, volume):
        if not volume['name']:
            raise ValidationException('Name must not be empty.', 'name')

        if not volume['type']:
            raise ValidationException('Type must not be empty.', 'type')

        profile_id = parse('profileId').find(volume)
        if profile_id:
            profile_id = profile_id[0].value
            profile = ModelImporter.model('aws', 'cumulus').load(profile_id,
                                                        user=getCurrentUser())

            if not profile:
                raise ValidationException('Invalid profile id')

            volume['profileId'] = profile['_id']

        volume_adapter = get_volume_adapter(volume)
        volume = volume_adapter.validate()

        return volume

    def filter(self, volume, user):
        volume = super(Volume, self).filter(doc=volume, user=user)

        # Convert status (IntEnum) to string
        volume['status'] = str(volume['status'])

        return volume

    def create_ebs(self, user, profileId, name, zone, size, fs):
        volume = {
            'name': name,
            'zone': zone,
            'size': size,
            'type': VolumeType.EBS,
            'ec2': {
                'id': None
            },
            'profileId': profileId,
            'status': VolumeState.CREATED,
            'log': []
        }

        if fs:
            volume['fs'] = fs

        # Add userId field to make search for a user volumes easier
        volume['userId'] = user['_id']

        self.setUserAccess(volume, user=user, level=AccessType.ADMIN)
        group = {
            '_id': ObjectId(self.get_group_id())
        }
        self.setGroupAccess(volume, group, level=AccessType.ADMIN)

        self.save(volume)
        send_status_notification('volume', volume)

        return volume

    def append_to_log(self, user, id, record):
        volume = self.load(id, user=user, level=AccessType.WRITE)
        self.update({'_id': ObjectId(id)}, {'$push': {'log': record}})
        send_log_notification('volume', volume, record)

    def update_volume(self, user, volume):
        volume_id = volume['_id']
        current_volume = self.load(volume_id, user=user,
                                   level=AccessType.WRITE)

        previous_status = current_volume['status']
        current_volume.update(volume)

        if current_volume['status'] != previous_status:
            send_status_notification('volume', current_volume)

        return self.save(current_volume)

    def log_records(self, user, id, offset=0):
        volume = self.load(id, user=user, level=AccessType.READ)

        return volume['log'][offset:]
