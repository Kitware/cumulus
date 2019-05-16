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

from girder.models.model_base import AccessControlledModel
from girder.utility.model_importer import ModelImporter
import cumulus


class BaseModel(AccessControlledModel):

    def __init__(self):
        super(BaseModel, self).__init__()
        self._group_id = None

    def get_group_id(self):

        if not self._group_id:
            group = ModelImporter.model('group').find({
                'name': cumulus.config.girder.group
            })

            if group.count() != 1:
                raise Exception('Unable to load group "%s"'
                                % cumulus.config.girder.group)

            self._group_id = group.next()['_id']

        return self._group_id

    def filter(self, doc, user):
        doc = super(BaseModel, self).filter(doc=doc, user=user)
        doc.pop('_accessLevel', None)
        doc.pop('_modelType', None)

        return doc
