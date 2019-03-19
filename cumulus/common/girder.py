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

from __future__ import absolute_import
import datetime
from bson.objectid import ObjectId, InvalidId

from girder.api.rest import ModelImporter, RestException, getCurrentUser
from girder.models.model_base import ValidationException
from girder.constants import AccessType

import cumulus
from cumulus.constants import ClusterType


def get_task_token(cluster=None):
    """
    Gets a Girder token to use to access Girder while running a task. By default
    we create a token using the cumulus girder user ( this user has certain
    privileges, such as access to passphrases that a regular user doesn't have).
    However, in the case of a NEWT cluster we need the token to be associated
    with the logged in user as this is used to look up the NEWT session ID.
    """
    if cluster and cluster['type'] == ClusterType.NEWT:
        user = getCurrentUser()
    else:
        user = ModelImporter.model('user') \
            .find({'login': cumulus.config.girder.user})

        if user.count() != 1:
            raise Exception('Unable to load user "%s"' %
                            cumulus.config.girder.user)

        user = user.next()

    return ModelImporter.model('token').createToken(user=user, days=7)


# This function should only be called from within the cumulus_plugin
# Otherwise, the aws model will not be registered.
def _get_profile(profile_id):
    user = getCurrentUser()
    query = {'userId': user['_id']}
    try:
        query['_id'] = ObjectId(profile_id)
    except InvalidId:
        query['name'] = profile_id

    profile = ModelImporter.model('aws', 'cumulus').findOne(query)
    secret = profile['secretAccessKey']

    profile = ModelImporter.model('aws', 'cumulus').filter(profile, user)

    if profile is None:
        raise ValidationException('Profile must be specified!')

    profile['_id'] = str(profile['_id'])

    return profile, secret


def create_notification(resource_name, notif_type, notification, user):
    expires = datetime.datetime.utcnow() + datetime.timedelta(seconds=30)
    type = '%s.%s' % (resource_name, notif_type)
    ModelImporter.model('notification').createNotification(
        type=type, data=notification, user=user, expires=expires)


def create_notifications(resource_name, notif_type, notification, resource):
    # Send notification to all users with admin access to this resource
    # we need to do this as the user updating status will not always be the
    # owner (the one with admin access)
    for user_access in resource['access']['users']:
        if user_access['level'] == AccessType.ADMIN:
            create_notification(resource_name, notif_type, notification,
                                {'_id': user_access['id']})


def send_status_notification(resource_type, resource):
    # For some statuses (on ec2 clusters) status can be an enum
    notification = {
        '_id': resource['_id'],
        'status': resource['status']
    }

    create_notifications(resource_type, 'status', notification, resource)


def send_log_notification(resource_name, resource, log):
    notification = {
        '_id': resource['_id'],
        'log': log,
    }
    create_notifications(resource_name, 'log', notification, resource)


def _get_group_id(group):
    group = ModelImporter.model('group').find({'name': group})

    if group.count() != 1:
        raise Exception('Unable to load group "%s"' % group)

    return group.next()['_id']


def check_group_membership(user, group):
    group_id = _get_group_id(group)
    if 'groups' not in user or ObjectId(group_id) not in user['groups']:
        raise RestException('The user is not in the required group.',
                            code=403)
