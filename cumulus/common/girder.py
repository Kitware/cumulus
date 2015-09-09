from __future__ import absolute_import
import datetime

from girder.api.rest import ModelImporter
from girder.constants import AccessType

import cumulus


def get_task_token():
    user = ModelImporter.model('user') \
        .find({'login': cumulus.config.girder.user})

    if user.count() != 1:
        raise Exception('Unable to load user "%s"' %
                        cumulus.config.girder.user)

    user = user.next()

    return ModelImporter.model('token').createToken(user=user, days=7)


def create_status_notification(resource_name, notification, user):
    expires = datetime.datetime.utcnow() + datetime.timedelta(seconds=30)
    type = '%s.status' % resource_name
    ModelImporter.model('notification').createNotification(
        type=type, data=notification, user=user, expires=expires)


def create_status_notifications(resource_name, notification, resource):
    # Send notification to all users with admin access to this resource
    # we need todo this as the user updating status will not always be the
    # owner (the one with admin access)
    for user_access in resource['access']['users']:
        if user_access['level'] == AccessType.ADMIN:
            create_status_notification(resource_name, notification, {
                '_id': user_access['id']
            })
