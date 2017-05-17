#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2016 Kitware Inc.
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

from girder.models.model_base import ValidationException


def to_object_id(id):
    if id and type(id) is not ObjectId:
        try:
            id = ObjectId(id)
        except Exception:
            raise ValidationException('Invalid ObjectId: %s' % id)

    return id


def merge_access(target, members, level, flags):
    """
    :param target: array of acces objects{id, level, flags}...
    :param members: array of ids
    :param level: number, AccessType [-1..2]
    :param flags: array of strings
    """
    new_members = []
    target_ids = [str(item['id']) for item in target]
    for member_id in members:
        # append member not in the target
        if member_id not in target_ids:
            access_object = {
                'id': to_object_id(member_id),
                'level': level,
                'flags': flags
            }
            target.append(access_object)
            new_members.append(member_id)
        # update member if it's in the target
        else:
            for item in target:
                if member_id == item['id']:
                    item['level'] = level
                    item['flags'] = flags
                    break
    return new_members