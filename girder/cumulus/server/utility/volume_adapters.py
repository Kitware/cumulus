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

from jsonpath_rw import parse

from girder.utility.model_importer import ModelImporter
from girder.models.model_base import ValidationException
from cumulus.constants import VolumeType
from girder.api.rest import getCurrentUser


class AbstractVolumeAdapter(ModelImporter):
    """
    This defines the interface to be used by all volume adapters.
    """
    def __init__(self, volume):
        self.volume = volume

    def validate(self):
        """
        Adapters may implement this if they need to perform any validation
        steps whenever the volume info is saved to the database. It should
        return the document with any necessary alterations in the success case,
        or throw an exception if validation fails.
        """

        return self.volume

    def delete(self):
        """
        Adapters may implement this if they support a delete operation.
        """
        pass


class EbsVolumeAdapter(AbstractVolumeAdapter):

    def validate(self):
        profile_id = parse('profileId').find(self.volume)[0].value
        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())

        if not profile:
            raise ValidationException('Invalid profile id')

        valid_fs = ['ext2', 'ext3', 'ext4']

        if 'fs' in self.volume and self.volume['fs'] not in valid_fs:
            raise ValidationException('Unsupported file system type', 'fs')

        try:
            int(self.volume['size'])
        except ValueError:
            raise ValidationException('size number in an integer', 'size')

        # Name should be unique
        user = getCurrentUser()
        query = {
            'name': self.volume['name'],
            'userId': user['_id']
        }

        if '_id' in self.volume:
            query['_id'] = {'$ne': self.volume['_id']}

        volume = self.model('volume', 'cumulus').findOne(query)
        if volume:
            raise ValidationException('A volume with that name already exists',
                                      'name')

        return self.volume


type_to_adapter = {
    VolumeType.EBS: EbsVolumeAdapter
}


def get_volume_adapter(volume):
    global type_to_adapter

    return type_to_adapter[volume['type']](volume)
