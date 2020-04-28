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

from jsonpath_rw import parse
from girder.models.model_base import ValidationException
from bson.objectid import ObjectId, InvalidId
from girder.constants import AccessType
from girder.api.rest import RestException, getCurrentUser
from girder.utility.model_importer import ModelImporter
from .base import BaseModel
from cumulus.constants import ClusterType, ClusterStatus, QueueType

from ..utility.cluster_adapters import get_cluster_adapter
from cumulus.common.girder import send_status_notification, \
    send_log_notification, check_group_membership
import cumulus
from cumulus import queue
import six


class Cluster(BaseModel):

    def __init__(self):
        super(Cluster, self).__init__()

    def initialize(self):
        self.name = 'clusters'

        self.exposeFields(level=AccessType.READ,
                          fields=('_id', 'status', 'name', 'config',
                                  'template', 'profileId',
                                  'type', 'userId', 'assetstoreId', 'volumes'))

    def load(self, id, level=AccessType.ADMIN, user=None, objectId=True,
             force=False, fields=None, exc=False):
        model = super(Cluster, self).load(id, level=level, user=user,
                                          objectId=objectId, force=force,
                                          fields=fields, exc=exc)
        return model

    def find_cluster(self, params, user, **kwargs):
        if params is None:
            params = {}

        query = {}
        if 'type' in params:
            query['type'] = params['type']

        limit = int(params.get('limit', 50))
        clusters = self.findWithPermissions(query, limit=limit, user=user,
                                            level=AccessType.ADMIN, **kwargs)

        return [self.filter(cluster, user) for cluster in clusters]

    def filter(self, cluster, user, passphrase=True):
        cluster = super(Cluster, self).filter(doc=cluster, user=user)

        if parse('config.ssh.passphrase').find(cluster) and passphrase:
            try:
                check_group_membership(user, cumulus.config.girder.group)
            except RestException:
                del cluster['config']['ssh']['passphrase']

        return cluster

    def validate(self, cluster):
        if not cluster['name']:
            raise ValidationException('Name must not be empty.', 'name')

        if not cluster['type']:
            raise ValidationException('Type must not be empty.', 'type')

        scheduler_type = parse('config.scheduler.type').find(cluster)
        if scheduler_type:
            scheduler_type = scheduler_type[0].value
        else:
            scheduler_type = QueueType.SGE
            config = cluster.setdefault('config', {})
            scheduler = config.setdefault('scheduler', {})
            scheduler['type'] = scheduler_type

        if not queue.is_valid_type(scheduler_type):
            raise ValidationException('Unsupported scheduler.', 'type')

        # If inserting, ensure no other clusters have the same name field amd
        # type
        if '_id' not in cluster:
            query = {
                'name': cluster['name'],
                'userId': getCurrentUser()['_id'],
                'type': cluster['type']
            }

            if self.findOne(query):
                raise ValidationException('A cluster with that name already '
                                          'exists', 'name')

        adapter = get_cluster_adapter(cluster)

        return adapter.validate()

    def _create(self, user, cluster):
        cluster = self.setUserAccess(cluster, user=user, level=AccessType.ADMIN)
        group = {
            '_id': ObjectId(self.get_group_id())
        }
        cluster = self.setGroupAccess(cluster, group, level=AccessType.ADMIN)

        # Add userId field to indicate ownership
        cluster['userId'] = user['_id']

        self.save(cluster)

        send_status_notification('cluster', cluster)

        return cluster

    def create_ansible(self, user, name, config, spec, launch_params, profile,
                       cluster_type=ClusterType.ANSIBLE):
        try:
            query = {
                'userId': user['_id'],
                '_id':  ObjectId(profile)}
        except InvalidId:
            query = {
                'userId': user['_id'],
                'name': profile}

        profile = ModelImporter.model('aws', 'cumulus').findOne(query)

        if profile is None:
            raise ValidationException('Profile must be specified!')

        # Should do some template validation here

        cluster = {
            'name': name,
            'profileId': profile['_id'],
            'log': [],
            'status': ClusterStatus.CREATED,
            'config': dict(config, **{
                'scheduler': {
                    'type': 'sge'
                },
                'ssh': {
                    'user': 'ubuntu',
                    'key': str(profile['_id'])
                },
                'launch': {
                    'spec': spec,
                    'params': launch_params
                }
            }),
            'type': cluster_type
        }

        return self._create(user, cluster)

    def create_traditional(self, user, name, config):
        cluster = {
            'name': name,
            'log': [],
            'status': 'creating',
            'config': config,
            'type': ClusterType.TRADITIONAL
        }

        # Set the key name
        cluster = self._create(user, cluster)
        cluster['config']['ssh']['key'] = str(cluster['_id'])
        self.save(cluster)

        return cluster

    def create_newt(self, user, name, config):

        scheduler = parse('scheduler.type').find(config)
        if not scheduler:
            config.setdefault('scheduler', {})['type'] = QueueType.SLURM
        cluster = {
            'name': name,
            'log': [],
            'status': 'creating',
            'config': config,
            'type': ClusterType.NEWT
        }

        return self._create(user, cluster)

    def append_to_log(self, user, id, record):
        def mongo_safe_value(value):
            new_value = {}

            if not isinstance(value, dict):
                return value
            else:
                for (k, v) in six.iteritems(value):
                    if '.' in str(k) or '$' in str(k):
                        k = k.replace('.', '\\u002e').replace('$', '\\u0024')

                    new_value[k] = mongo_safe_value(v)

            return new_value
        # Load first to force access check
        log = mongo_safe_value(record)
        cluster = self.load(id, user=user, level=AccessType.WRITE)
        self.update({'_id': ObjectId(id)},
                    {'$push': {
                        'log': mongo_safe_value(record)
                    }})
        send_log_notification('cluster', cluster, log)

    def update_status(self, id, status):
        self.update({'_id': ObjectId(id)},
                    {'$set': {
                        'status': status
                    }})

    def update_cluster(self, user, cluster):
        # Load first to force access check
        cluster_id = cluster['_id']
        current_cluster = self.load(cluster_id, user=user,
                                    level=AccessType.WRITE)

        previous_status = current_cluster['status']
        current_cluster.update(cluster)

        # If the status has changed create a notification
        if current_cluster['status'] != previous_status:
            send_status_notification('cluster', current_cluster)

        return self.save(current_cluster)

    def log_records(self, user, id, offset=0):
        # TODO Need to figure out perms a remove this force
        cluster = self.load(id, user=user, level=AccessType.READ)

        return cluster['log'][offset:]

    def delete(self, user, id):
        cluster = self.load(id, user=user, level=AccessType.ADMIN)

        self.remove(cluster)
