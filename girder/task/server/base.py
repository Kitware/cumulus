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

from girder.api.rest import Resource, RestException
from bson.objectid import ObjectId
import cumulus


class BaseResource(Resource):

    def _get_group_id(self, group):
        group = self.model('group').find({'name': group})

        if group.count() != 1:
            raise Exception('Unable to load group "%s"' % group)

        return group.next()['_id']

    def check_group_membership(self, user, group):
        group_id = self._get_group_id(group)
        if 'groups' not in user or ObjectId(group_id) not in user['groups']:
            raise RestException('The user is not in the required group.',
                                code=403)

    def get_task_token(self):
        user = self.model('user').find({'login': cumulus.config.girder.user})

        if user.count() != 1:
            raise Exception('Unable to load user "%s"'
                            % cumulus.config.girder.user)

        user = user.next()

        return self.model('token').createToken(user=user, days=7)
