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

from girder.models.model_base import ValidationException
from bson.objectid import ObjectId
from girder.constants import AccessType
from .base import BaseModel
from cumulus.common.girder import create_status_notifications


class Job(BaseModel):

    def __init__(self):
        super(Job, self).__init__()

    def initialize(self):
        self.name = 'jobs'

    def validate(self, doc):
        if not doc['name']:
            raise ValidationException('Name must not be empty.', 'name')

        return doc

    def create(self, user, job):

        job['status'] = 'created'
        job['log'] = []

        self.setUserAccess(job, user=user, level=AccessType.ADMIN)
        group = {
            '_id': ObjectId(self.get_group_id())
        }
        doc = self.setGroupAccess(job, group, level=AccessType.ADMIN)

        self.save(job)

        return doc

    def status(self, user, id):
        job = self.load(id, user=user, level=AccessType.READ)

        return job['status']

    def update_status(self, user, id, status):
        # Load first to force access check
        job = self.load(id, user=user, level=AccessType.WRITE)

        if job['status'] != status:
            notification = {
                '_id': id,
                'status': status
            }
            create_status_notifications('job', notification, job)

        if status:
            job['status'] = status

        return self.save(job)

    def update_job(self, user, job):
        job_id = job['_id']
        current_job = self.load(job_id, user=user, level=AccessType.WRITE)
        new_status = job['status']

        if current_job['status'] != new_status:
            notification = {
                '_id': job_id,
                'status': new_status
            }
            create_status_notifications('job', notification, current_job)

        return self.save(job)

    def add_log_record(self, user, _id, record):
        # Load first to force access check
        self.load(_id, user=user, level=AccessType.WRITE)
        self.update({'_id': ObjectId(_id)}, {'$push': {'log': record}})

    def log_records(self, user, id, offset=0):
        job = self.load(id, user=user, level=AccessType.READ)

        return job['log'][offset:]
