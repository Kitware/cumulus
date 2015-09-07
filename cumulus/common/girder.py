from __future__ import absolute_import
from girder.api.rest import ModelImporter

import cumulus


def get_task_token():
    user = ModelImporter.model('user') \
        .find({'login': cumulus.config.girder.user})

    if user.count() != 1:
        raise Exception('Unable to load user "%s"' %
                        cumulus.config.girder.user)

    user = user.next()

    return ModelImporter.model('token').createToken(user=user, days=7)
