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
from girder.constants import AccessType

from cumulus.common.girder import create_status_notifications


class Task(AccessControlledModel):

    def initialize(self):
        self.name = 'tasks'

    def validate(self, doc):
        return doc

    def create(self, user, task):

        doc = self.setUserAccess(task, user, level=AccessType.ADMIN, save=True)

        return doc

    def update_task(self, user, task):
        task_id = task['_id']
        current_task = self.load(task_id, level=AccessType.ADMIN, user=user)
        new_status = task['status']
        if current_task['status'] != new_status:
            notification = {
                '_id': task_id,
                'status': new_status
            }
            create_status_notifications('task', notification, current_task)

        return self.save(task)
