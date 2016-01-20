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

import six

from girder.models.model_base import AccessControlledModel
from girder.constants import AccessType

class Task(AccessControlledModel):

    def initialize(self):
        self.name = 'tasks'
        self.ensureIndices(['taskFlowId', 'celeryTaskId'])

    def validate(self, doc):
        return doc

    def create(self, user, taskflow, task):
        """
        Create a new task associated with a taskflow

        :param user: The user creating the task.
        :param taskflow: The taskflow that the task will be part of.
        :param task: The task document.
        """

        task['taskFlowId'] = taskflow['_id']
        task['status'] = 'created'
        doc = self.setUserAccess(task, user, level=AccessType.ADMIN, save=True)

        return doc

    def find_by_celery_task_id(self, user, celery_task_id):
        """
        Lookup a task by its celery task id

        :param user: The user to user for access
        :param celery_task_id: The celery task id.
        """
        query = {
            'celeryTaskId': celery_task_id
        }
        try:
            # TODO workout why six doesn't work here
            cursor = self.find(query=query, limit=1)
            task = cursor.next()
        except StopIteration:
            raise Exception(
                'Unable to retrive take celery task id: %s' % celery_task_id)

        return task

    def find_by_taskflow_id(self, user, taskflow_id, fields=None):
        """
        Lookup all tasks associated with a taskflow

        :param user: The user to user for access
        :param taskflow_id: The taskflow id.
        """
        query = {
            'taskFlowId': taskflow_id
        }
        return  self.find(query=query, fields=fields)

    def update_task(self, user, task):
        return self.save(task)
