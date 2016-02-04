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

from girder.models.model_base import AccessControlledModel
from girder.constants import AccessType

class Taskflow(AccessControlledModel):

    def initialize(self):
        self.name = 'taskflows'
        self.exposeFields(level=AccessType.READ, fields=(
            '_id', 'status', 'log', 'activeTaskCount', 'taskFlowClass'))

    def validate(self, doc):
        return doc

    def create(self, user, taskflow):

        taskflow = self.setUserAccess(
            taskflow, user, level=AccessType.ADMIN, save=True)

        return taskflow

    def append_to_log(self, taskflow, log):
        """
        Append a log entry to the taskflows log
        """
        # This needs to be done in the database to prevent lost updates
        query = {
            '_id': taskflow['_id']
        }
        update = {
            '$push': {
                'log': log
            }
        }

        return self.update(query, update, multi=False)

    def _to_paths(self, d, path=''):
        """
        Utility method to convert and dictionary into 'path' 'value' pairs
        that can be passed to $set operator
        """
        for k, v in d.iteritems():
            if isinstance(v, dict):
                if not path:
                    new_path = k
                else:
                    new_path = '%s.%s' % (path, k)

                for path_value in self._to_paths(v, new_path):
                    yield path_value
            else:
                if not path:
                    yield (k, v)
                else:
                    yield ('%s.%s' % (path, k), v)

    def update_taskflow(self, user, taskflow, updates):
        """
        Use $set operator to update values on taskflow, we need to use $set
        to prevent lot update ...
        """
        query = {
            '_id': taskflow['_id']
        }
        update = {
            '$set': { }
        }

        for (path, value) in self._to_paths(updates):
            update['$set'][path] = value

        self.update(query, update, multi=False)

        return self.load(taskflow['_id'], user=user)

    def get_path(self, taskflow, path):
        """
        Get the value at a particular 'path' on the taskflow.
        """
        query = {
            '_id': taskflow['_id']
        }
        projection = {
            path: 1
        }

        return self.findOne(query=query, fields=projection)

    def delete(self, taskflow):
        """
        Delete a taskflow and its associated tasks.
        """
        query = {
            'taskFlowId': taskflow['_id']
        }

        self.model('task', 'taskflow').removeWithQuery(query)
        self.remove(taskflow)
