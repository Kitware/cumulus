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
from girder.utility.model_importer import ModelImporter
from girder import events

from cumulus.taskflow import TaskFlowState, TaskState
from cumulus.common.girder import send_status_notification, \
    send_log_notification
import six

from ..utility import to_object_id, merge_access, \
    set_access_for_list, patch_access_for_list, revoke_access_for_list

MAX_RETRIES = 4


class Taskflow(AccessControlledModel):

    def initialize(self):
        self.name = 'taskflows'
        self.exposeFields(level=AccessType.READ, fields=(
            '_id', 'status', 'log', 'activeTaskCount', 'taskFlowClass',
            'meta'))

    def validate(self, doc):
        return doc

    def create(self, user, taskflow):
        taskflow['status'] = TaskFlowState.CREATED
        taskflow['log'] = []

        taskflow = self.setUserAccess(
            taskflow, user, level=AccessType.ADMIN, save=True)

        send_status_notification('taskflow', taskflow)

        return taskflow

    def set_access(self, user, taskflow, users, groups):
        access_list = {'groups': [], 'users': []}

        for user_id in users:
            access_object = {
                'id': to_object_id(user_id),
                'level': AccessType.READ
            }
            access_list['users'].append(access_object)

        for group_id in groups:
            access_object = {
                'id': to_object_id(group_id),
                'level': AccessType.READ
            }
            access_list['groups'].append(access_object)

        task_model = ModelImporter.model('task', 'taskflow')
        tasks = task_model.find_by_taskflow_id(user, taskflow['_id'])
        set_access_for_list(user, task_model, tasks, access_list)

        job_model = ModelImporter.model('job', 'cumulus')
        jobs = taskflow.get('meta', {}).get('jobs', [])
        set_access_for_list(user, job_model, jobs, access_list)

        return self.setAccessList(taskflow, access_list, save=True)

    def patch_access(self, user, taskflow, users, groups):
        access_list = taskflow.get('access', {'groups': [], 'users': []})

        merge_access(access_list['users'], users, AccessType.READ, [])
        merge_access(access_list['groups'], groups, AccessType.READ, [])

        # add access to tasks
        task_model = ModelImporter.model('task', 'taskflow')
        tasks = task_model.find_by_taskflow_id(user, taskflow['_id'])
        patch_access_for_list(user, task_model, tasks, users, groups)

        # add access to jobs
        job_model = ModelImporter.model('job', 'cumulus')
        jobs = taskflow.get('meta', {}).get('jobs', [])
        patch_access_for_list(user, job_model, jobs, users, groups)

        return self.setAccessList(taskflow, access_list, save=True)

    def revoke_access(self, user, taskflow, users, groups):
        tf_access_list = taskflow.get('access', {'groups': [], 'users': []})
        access_list = {
            'groups': [g for g in tf_access_list['groups']
                       if str(g['id']) not in groups],
            'users': [u for u in tf_access_list['users']
                      if str(u['id']) not in users]
        }

        # revoke tasks
        task_model = ModelImporter.model('task', 'taskflow')
        tasks = task_model.find_by_taskflow_id(user, taskflow['_id'])
        revoke_access_for_list(user, task_model, tasks, users, groups)

        # revoke jobs
        job_model = ModelImporter.model('job', 'cumulus')
        jobs = taskflow.get('meta', {}).get('jobs', [])
        revoke_access_for_list(user, job_model, jobs, users, groups)

        return self.setAccessList(taskflow, access_list, save=True, force=True)

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

        result = self.update(query, update, multi=False)
        send_log_notification('taskflow', taskflow, log)
        return result

    def _to_paths(self, d, path=''):
        """
        Utility method to convert and dictionary into 'path' 'value' pairs
        that can be passed to $set operator
        """
        for k, v in six.iteritems(d):
            if isinstance(v, dict) and v:
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
            '$set': {}
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

        ModelImporter.model('task', 'taskflow').removeWithQuery(query)
        self.remove(taskflow)

    def status(self, user, taskflow):
        """
        Utility function to extract the status.
        """
        if 'status' in taskflow:
            if taskflow['status'] == TaskFlowState.TERMINATING:
                if taskflow['activeTaskCount'] == 0:
                    return TaskFlowState.TERMINATED
                else:
                    return TaskFlowState.TERMINATING

            if taskflow['status'] in [TaskFlowState.DELETED,
                                      TaskFlowState.DELETING]:
                return taskflow['status']

        tasks = ModelImporter.model('task', 'taskflow').find_by_taskflow_id(
            user, taskflow['_id'])

        task_status = [t['status'] for t in tasks]
        task_status = set(task_status)

        status = TaskFlowState.CREATED
        if len(task_status) == 1:
            status = task_status.pop()
        elif TaskState.ERROR in task_status:
            status = TaskFlowState.ERROR
        elif TaskState.RUNNING in task_status or \
            (TaskState.COMPLETE in task_status and
             TaskState.CREATED in task_status):
            status = TaskFlowState.RUNNING

        return status

    def update_state(self, user, taskflow_id):
        """
        Update the state of the taskflow. This is called any time a task in the
        flow updates its state.
        """
        taskflow = self.load(taskflow_id, level=AccessType.WRITE, user=user)

        # Use a 'update if current' strategy to update the taskflow state. This
        # should work provided we don't have high contention, if we start to
        # see high contention we will need to rethink this.
        new_status = self.status(user, taskflow)
        # We have nothing todo
        if taskflow['status'] == new_status:
            return

        query = {
            '_id': taskflow['_id'],
            'status': taskflow['status']
        }
        update = {
            '$set': {'status': new_status}
        }

        retries = 0
        while True:
            update_result = self.update(query, update, multi=False)

            if update_result.modified_count > 0 or \
                    update_result.matched_count == 0:
                break

            if retries < MAX_RETRIES:
                retries += 1
            else:
                raise Exception('Max retry count exceeded.')

        if taskflow['status'] != new_status:
            taskflow['status'] = new_status
            events.trigger('cumulus.taskflow.status_update', {'taskflow': taskflow})
            send_status_notification('taskflow', taskflow)
