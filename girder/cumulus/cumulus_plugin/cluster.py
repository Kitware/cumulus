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

import cherrypy
import json
from jsonpath_rw import parse
from bson.objectid import ObjectId

from girder import events
from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType, TokenScope
from girder.api.docs import addModel
from girder.api.rest import RestException, getBodyJson, loadmodel
from girder.models.model_base import ValidationException
from girder.utility.model_importer import ModelImporter
from .base import BaseResource
from cumulus.constants import ClusterType, ClusterStatus
from .utility.cluster_adapters import get_cluster_adapter
from cumulus.ssh.tasks.key import generate_key_pair
from cumulus.common import update_dict
from cumulus.common.jsonpath import get_property


class Cluster(BaseResource):

    def __init__(self):
        super(Cluster, self).__init__()
        self.resourceName = 'clusters'
        self.route('POST', (), self.create)
        self.route('POST', (':id', 'log'), self.handle_log_record)
        self.route('GET', (':id', 'log'), self.log)
        self.route('PUT', (':id', 'start'), self.start)
        self.route('PUT', (':id', 'launch'), self.launch)
        self.route('PUT', (':id', 'provision'), self.provision)
        self.route('PATCH', (':id',), self.update)
        self.route('GET', (':id', 'status'), self.status)
        self.route('PUT', (':id', 'terminate'), self.terminate)
        self.route('PUT', (':id', 'job', ':jobId', 'submit'), self.submit_job)
        self.route('GET', (':id', ), self.get)
        self.route('DELETE', (':id', ), self.delete)
        self.route('GET', (), self.find)

        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = ModelImporter.model('cluster', 'cumulus')

    @access.user(scope=TokenScope.DATA_WRITE)
    def handle_log_record(self, id, params):
        user = self.getCurrentUser()

        if not self._model.load(id, user=user, level=AccessType.ADMIN):
            raise RestException('Cluster not found.', code=404)

        return self._model.append_to_log(
            user, id, getBodyJson())

    handle_log_record.description = None

    def _create_ec2(self, params, body):
        return self._create_ansible(params, body, cluster_type=ClusterType.EC2)

    def _create_ansible(self, params, body, cluster_type=ClusterType.ANSIBLE):

        self.requireParams(['name', 'profileId'], body)

        name = body['name']
        playbook = get_property('config.launch.spec', body, default='default')
        launch_params = get_property('config.launch.params', body, default={})
        config = get_property('config', body, default={})
        profile_id = body['profileId']
        user = self.getCurrentUser()

        cluster = self._model.create_ansible(user, name, config, playbook,
                                             launch_params, profile_id,
                                             cluster_type=cluster_type)

        return cluster

    def _create_traditional(self, params, body):

        self.requireParams(['name', 'config'], body)
        self.requireParams(['ssh', 'host'], body['config'])
        self.requireParams(['user'], body['config']['ssh'])

        name = body['name']
        config = body['config']
        user = self.getCurrentUser()

        cluster = self._model.create_traditional(user, name, config)

        # Fire off job to create key pair for cluster
        girder_token = self.get_task_token()['_id']
        generate_key_pair.delay(
            self._model.filter(cluster, user),
            girder_token)

        return cluster

    def _create_newt(self, params, body):

        self.requireParams(['name', 'config'], body)
        self.requireParams(['host'], body['config'])

        name = body['name']
        config = body['config']
        user = self.getCurrentUser()
        config['user'] = user['login']

        cluster = self._model.create_newt(user, name, config)

        return cluster

    @access.user(scope=TokenScope.DATA_WRITE)
    def create(self, params):
        body = getBodyJson()
        # Default ec2 cluster
        cluster_type = 'ec2'

        if 'type' in body:
            if not ClusterType.is_valid_type(body['type']):
                raise RestException('Invalid cluster type.', code=400)
            cluster_type = body['type']

        if cluster_type == ClusterType.EC2:
            cluster = self._create_ec2(params, body)
        elif cluster_type == ClusterType.ANSIBLE:
            cluster = self._create_ansible(params, body)
        elif cluster_type == ClusterType.TRADITIONAL:
            cluster = self._create_traditional(params, body)
        elif cluster_type == ClusterType.NEWT:
            cluster = self._create_newt(params, body)
        else:
            raise RestException('Invalid cluster type.', code=400)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/clusters/%s' % cluster['_id']

        return self._model.filter(cluster, self.getCurrentUser())

    addModel('Id', {
        'id': 'Id',
        'properties': {
            '_id': {'type': 'string', 'description': 'The id.'}
        }
    }, 'clusters')

    addModel('UserNameParameter', {
        'id': 'UserNameParameter',
        'properties': {
            'user': {'type': 'string', 'description': 'The ssh user id'}
        }
    }, 'clusters')

    addModel('SshParameters', {
        'id': 'SshParameters',
        'properties': {
            'ssh': {
                '$ref': '#/definitions/UserNameParameter'
            }
        }
    }, 'clusters')

    addModel('ClusterParameters', {
        'id': 'ClusterParameters',
        'required': ['name', 'config', 'type'],
        'properties': {
            'name': {'type': 'string',
                     'description': 'The name to give the cluster.'},
            'template':  {'type': 'string',
                          'description': 'The cluster template to use. '
                          '(ec2 only)'},
            'config': {
                '$ref': '#/definitions/SshParameters',
                'host': {'type': 'string',
                         'description': 'The hostname of the head node '
                                        '(trad only)'}
            },
            'type': {'type': 'string',
                     'description': 'The cluster type, either "ec2" or "trad"'}

        }}, 'clusters')

    create.description = (Description(
        'Create a cluster'
    )
        .param(
            'body',
            'The name to give the cluster.',
            dataType='ClusterParameters',
            required=True, paramType='body'))

    def _get_body(self):
        body = {}
        if cherrypy.request.body:
            request_body = cherrypy.request.body.read().decode('utf8')
            if request_body:
                body = json.loads(request_body)

        return body

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='cluster', plugin='cumulus', level=AccessType.ADMIN)
    def start(self, cluster, params):
        body = self._get_body()
        adapter = get_cluster_adapter(cluster)
        adapter.start(body)
        events.trigger('cumulus.cluster.started', info=cluster)

    addModel('ClusterOnStartParms', {
        'id': 'ClusterOnStartParms',
        'properties': {
            'submitJob': {
                'pattern': '^[0-9a-fA-F]{24}$',
                'type': 'string',
                'description': 'The id of a Job to submit when the cluster '
                'is started.'
            }
        }
    }, 'clusters')

    addModel('ClusterStartParams', {
        'id': 'ClusterStartParams',
        'properties': {
            'onStart': {
                '$ref': '#/definitions/ClusterOnStartParms'
            }
        }
    }, 'clusters')

    start.description = (Description(
        'Start a cluster (ec2 only)'
    )
        .param(
            'id',
            'The cluster id to start.', paramType='path', required=True
        )
        .param(
            'body', 'Parameter used when starting cluster', paramType='body',
            dataType='ClusterStartParams', required=False))

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='cluster', plugin='cumulus', level=AccessType.ADMIN)
    def launch(self, cluster, params):

        # Update any launch parameters passed in message body
        body = self._get_body()
        cluster['config']['launch']['params'].update(body)
        cluster = self._model.save(cluster)

        return self._model.filter(
            self._launch_or_provision('launch', cluster),
            self.getCurrentUser())

    launch.description = (Description(
        'Start a cluster with ansible'
    ).param(
        'id',
        'The cluster id to start.', paramType='path', required=True))

    @access.user(scope=TokenScope.DATA_WRITE)
    @loadmodel(model='cluster', plugin='cumulus', level=AccessType.ADMIN)
    def provision(self, cluster, params):

        if not ClusterStatus.valid_transition(
                cluster['status'], ClusterStatus.PROVISIONING):
            raise RestException(
                'Cluster status is %s and cannot be provisioned' %
                cluster['status'], code=400)

        body = self._get_body()
        provision_ssh_user = get_property('ssh.user', body)
        if provision_ssh_user:
            cluster['config'].setdefault('provision', {})['ssh'] = {
                'user': provision_ssh_user
            }
            del body['ssh']

        if 'spec' in body:
            cluster['config'].setdefault('provision', {})['spec'] \
                = body['spec']
            del body['spec']

        cluster['config'].setdefault('provision', {})\
            .setdefault('params', {}).update(body)
        cluster = self._model.save(cluster)

        return self._model.filter(
            self._launch_or_provision('provision', cluster),
            self.getCurrentUser())

    provision.description = (Description(
        'Provision a cluster with ansible'
    ).param(
        'id',
        'The cluster id to provision.', paramType='path', required=True
    ).param(
        'body', 'Parameter used when provisioning cluster', paramType='body',
        dataType='list', required=False))

    def _launch_or_provision(self, process, cluster):
        assert process in ['launch', 'provision']
        adapter = get_cluster_adapter(cluster)

        return getattr(adapter, process)()

    @access.user(scope=TokenScope.DATA_WRITE)
    def update(self, id, params):
        body = getBodyJson()
        user = self.getCurrentUser()

        cluster = self._model.load(id, user=user, level=AccessType.WRITE)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        if 'assetstoreId' in body:
            cluster['assetstoreId'] = body['assetstoreId']

        if 'status' in body:
            if ClusterStatus.valid(body['status']):
                cluster['status'] = body['status']
            else:
                raise RestException('%s is not a valid cluster status' %
                                    body['status'], code=400)

        if 'timings' in body:
            if 'timings' in cluster:
                cluster['timings'].update(body['timings'])
            else:
                cluster['timings'] = body['timings']

        if 'config' in body:
            # Need to check we aren't try to update immutable fields
            immutable_paths = ['_id', 'ssh.user']
            for path in immutable_paths:
                if parse(path).find(body['config']):
                    raise RestException("The '%s' field can't be updated"
                                        % path)

            update_dict(cluster['config'], body['config'])

        cluster = self._model.update_cluster(user, cluster)

        # Now do any updates the adapter provides
        adapter = get_cluster_adapter(cluster)
        try:
            adapter.update(body)
        # Skip adapter.update if update not defined for this adapter
        except (NotImplementedError, ValidationException):
            pass

        return self._model.filter(cluster, user)

    addModel('ClusterUpdateParameters', {
        'id': 'ClusterUpdateParameters',
        'properties': {
            'status': {'type': 'string', 'enum': ['created', 'running',
                                                  'stopped', 'terminated'],
                       'description': 'The new status. (optional)'}
        }
    }, 'clusters')

    update.description = (Description(
        'Update the cluster'
    )
        .param('id',
               'The id of the cluster to update', paramType='path')
        .param(
            'body',
            'The properties to update.', dataType='ClusterUpdateParameters',
            paramType='body')
        .notes('Internal - Used by Celery tasks'))

    @access.user(scope=TokenScope.DATA_READ)
    def status(self, id, params):
        user = self.getCurrentUser()
        cluster = self._model.load(id, user=user, level=AccessType.READ)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        return {'status': cluster['status']}

    addModel('ClusterStatus', {
        'id': 'ClusterStatus',
        'required': ['status'],
        'properties': {
            'status': {'type': 'string',
                       'enum': [ClusterStatus.valid_transitions.keys()]}
        }
    }, 'clusters')

    status.description = (
        Description('Get the clusters current state')
        .param('id',
               'The cluster id to get the status of.', paramType='path')
        .responseClass('ClusterStatus'))

    @access.user(scope=TokenScope.DATA_WRITE)
    def terminate(self, id, params):
        user = self.getCurrentUser()
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        adapter = get_cluster_adapter(cluster)
        adapter.terminate()

    terminate.description = (Description(
        'Terminate a cluster'
    )
        .param(
            'id',
            'The cluster to terminate.', paramType='path'))

    @access.user(scope=TokenScope.DATA_READ)
    def log(self, id, params):
        user = self.getCurrentUser()
        offset = 0
        if 'offset' in params:
            offset = int(params['offset'])

        if not self._model.load(id, user=user, level=AccessType.READ):
            raise RestException('Cluster not found.', code=404)

        log_records = self._model.log_records(user, id, offset)

        return {'log': log_records}

    log.description = (Description(
        'Get log entries for cluster'
    )
        .param(
            'id',
            'The cluster to get log entries for.', paramType='path')
        .param(
            'offset',
            'The offset to start getting entries at.', required=False,
            paramType='query'))

    @access.user(scope=TokenScope.DATA_WRITE)
    def submit_job(self, id, jobId, params):
        job_id = jobId
        user = self.getCurrentUser()
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        if cluster['status'] != ClusterStatus.RUNNING:
            raise RestException('Cluster is not running', code=400)

        job_model = ModelImporter.model('job', 'cumulus')
        job = job_model.load(
            job_id, user=user, level=AccessType.ADMIN)

        # Set the clusterId on the job for termination
        job['clusterId'] = ObjectId(id)

        # Add any job parameters to be used when templating job script
        body = cherrypy.request.body.read().decode('utf8')
        if body:
            job['params'] = json.loads(body)

        job_model.save(job)

        cluster_adapter = get_cluster_adapter(cluster)
        del job['access']
        del job['log']
        cluster_adapter.submit_job(job)

    submit_job.description = (
        Description('Submit a job to the cluster')
        .param(
            'id',
            'The cluster to submit the job to.', required=True,
            paramType='path')
        .param(
            'jobId',
            'The cluster to get log entries for.', required=True,
            paramType='path')
        .param(
            'body',
            'The properties to template on submit.', dataType='object',
            paramType='body'))

    @access.user(scope=TokenScope.DATA_READ)
    def get(self, id, params):
        user = self.getCurrentUser()
        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)

        if not cluster:
            raise RestException('Cluster not found.', code=404)

        return self._model.filter(cluster, user)

    get.description = (
        Description('Get a cluster')
        .param(
            'id',
            'The cluster id.', paramType='path', required=True))

    @access.user(scope=TokenScope.DATA_WRITE)
    def delete(self, id, params):
        user = self.getCurrentUser()

        cluster = self._model.load(id, user=user, level=AccessType.ADMIN)
        if not cluster:
            raise RestException('Cluster not found.', code=404)

        adapter = get_cluster_adapter(cluster)
        adapter.delete()

        self._model.delete(user, id)

    delete.description = (
        Description('Delete a cluster and its configuration')
        .param('id', 'The cluster id.', paramType='path', required=True))

    @access.user(scope=TokenScope.DATA_READ)
    def find(self, params):
        return self._model.find_cluster(params, user=self.getCurrentUser())

    find.description = (
        Description('Search for clusters with certain properties')
        .param('type', 'The cluster type to search for', paramType='query',
               required=False)
        .param('limit', 'The max number of clusters to return',
               paramType='query', required=False, default=50))
