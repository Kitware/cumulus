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
from botocore.exceptions import ClientError, EndpointConnectionError
from jsonpath_rw import parse

from girder.constants import AccessType

from .base import BaseModel
from girder.models.model_base import ValidationException
from girder.api.rest import getCurrentUser
from cumulus.common.girder import create_notifications


class Rax(BaseModel):
    def __init__(self):
        super(Rax, self).__init__()

    def initialize(self):
        self.name = 'rax'
        self.ensureIndices(['userId', 'name'])
        self.exposeFields(level=AccessType.READ, fields=(
            '_id', 'name', 'userName', 'regionName', 'status',
            'errorMessage', 'cloudProvider'))

#    def _validate_region(self, client, doc):
#
#         try:
#             response = client.describe_regions(RegionNames=[doc['regionName']])
#             endpoint = parse('Regions[0].Endpoint').find(response)
#             if endpoint:
#                 endpoint = endpoint[0].value
#             else:
#                 raise ValidationException('Unable to extract region endpoint.')
#
#             doc['regionHost'] = endpoint
#         except ClientError as ce:
#             code = parse('Error.Code').find(ce.response)
#             if code:
#                 code = code[0].value
#             else:
#                 raise
#
#             if code == ClientErrorCode.InvalidParameterValue:
#                 raise ValidationException('Invalid region', 'regionName')
#         except EndpointConnectionError as ece:
#             raise ValidationException(ece.message)

#     def _validate_zone(self, client, doc):
#         try:
#             client = get_ec2_client(doc)
#             client.describe_availability_zones(
#                 ZoneNames=[doc['availabilityZone']])
#
#         except ClientError as ce:
#             code = parse('Error.Code').find(ce.response)
#             if code:
#                 code = code[0].value
#             else:
#                 raise
#
#             if code == ClientErrorCode.InvalidParameterValue:
#                 raise ValidationException(
#                     'Invalid zone', 'availabilityZone')
#         except EndpointConnectionError as ece:
#             raise ValidationException(ece.message)

    def validate(self, doc):
        return doc
#         name = doc['name']
#
#         if type(doc['publicIPs']) != bool:
#             raise ValidationException('Value must be of type boolean',
#                                       'publicIPs')
#
#         if not name:
#             raise ValidationException('A name must be provided', 'name')
#
#         # Check for duplicate names
#         query = {
#             'name': name,
#             'userId': doc['userId']
#         }
#         if '_id' in doc:
#             query['_id'] = {'$ne': doc['_id']}
#
#         if self.findOne(query):
#             raise ValidationException('A profile with that name already exists',
#                                       'name')
#
#         client = None
#         # First validate the credentials
#         try:
#             client = get_ec2_client(doc)
#             client.describe_account_attributes()
#         except ClientError as ce:
#             code = parse('Error.Code').find(ce.response)
#             if code:
#                 code = code[0].value
#             else:
#                 raise
#
#             if code == ClientErrorCode.AuthFailure:
#                 raise ValidationException('Invalid AWS credentials')
#         except EndpointConnectionError as ece:
#             raise ValidationException(ece.message)
#
#         # Now validate the region
#         self._validate_region(client, doc)
#
#         # Only do the rest of the validation if this is a new profile (not
#         # a key update )
#         if '_id' not in doc:
#             # Now validate the zone
#             self._validate_zone(client, doc)
#
#         return doc

    def create_profile(self, userId, profile_type, name, user_name,
                       apiKey, region_name):
        user = getCurrentUser()
        profile = {
            'name': name,
            'userName': user_name,
            'apiKey': apiKey,
            'cloudProvider': profile_type,
            'regionName': region_name,
            'userId': userId,
            'status': 'creating'
        }

        profile = self.setUserAccess(profile, user, level=AccessType.ADMIN,
                                     save=False)
        group = {
            '_id': ObjectId(self.get_group_id())
        }
        profile = self.setGroupAccess(profile, group, level=AccessType.ADMIN)

        return self.save(profile)

    def find_profiles(self, userId):
        query = {
            'userId': userId,
            'cloudProvider': 'rax'
        }

        return self.find(query)

    def update_rax_profile(self, user, profile):
        profile_id = profile['_id']
        current_profile = self.load(profile_id, user=user,
                                    level=AccessType.ADMIN)
        new_status = profile['status']
        if current_profile['status'] != new_status:
            notification = {
                '_id': profile_id,
                'status': new_status
            }
            create_notifications('profile', 'status', notification,
                                 current_profile)

        return self.save(profile)
