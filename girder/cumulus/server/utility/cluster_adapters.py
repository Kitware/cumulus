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

import base64
from jsonpath_rw import parse

from girder.utility.model_importer import ModelImporter
from girder.models.model_base import ValidationException
from girder.api.rest import RestException, getApiUrl, getCurrentUser

from cumulus.constants import ClusterType, ClusterStatus
from cumulus.common.girder import get_task_token, _get_profile
import cumulus.tasks.cluster
import cumulus.tasks.job
import cumulus.ansible.tasks.cluster
from cumulus.common.jsonpath import get_property


class AbstractClusterAdapter(ModelImporter):
    """
    This defines the interface to be used by all cluster adapters.
    """
    def __init__(self, cluster):
        self.cluster = cluster
        self._model = self.model('cluster', 'cumulus')

    def update_status(self, status):
        self.cluster = self._model.update_status(self.cluster, status)

    def validate(self):
        """
        Adapters may implement this if they need to perform any validation
        steps whenever the cluster info is saved to the database. It should
        return the document with any necessary alterations in the success case,
        or throw an exception if validation fails.
        """
        return self.cluster

    def start(self, request_body):
        """
        Adapters may implement this if they support a start operation.
        """
        raise ValidationException(
            'This cluster type does not support a start operation')

    def terminate(self):
        """
        Adapters may implement this if they support a terminate operation.
        """
        raise ValidationException(
            'This cluster type does not support a terminate operation')

    def update(self, request_body):
        """
        Adapters may implement this if they support a update operation.
        """
        raise ValidationException(
            'This cluster type does not support a update operation')

    def delete(self):
        """
        Adapters may implement this if they support a delete operation.
        """
        # If an assetstore was created for this cluster then try to remove it
        if 'assetstoreId' in self.cluster:
            try:
                assetstore = self.model('assetstore').load(
                    self.cluster['assetstoreId'])
                self.model('assetstore').remove(assetstore)
            except ValidationException:
                # If we still have files associated with the assetstore then
                # leave it.
                pass

    def submit_job(self, job):
        log_url = '%s/jobs/%s/log' % (getApiUrl(), job['_id'])

        girder_token = get_task_token()['_id']
        cumulus.tasks.job.submit(girder_token, self.cluster, job,
                                 log_url)


class AnsibleClusterAdapter(AbstractClusterAdapter):
    """
    This defines the interface to be used by all cluster adapters.
    """
    DEFAULT_PLAYBOOK = 'ec2'

    def update_status(self, status):
        assert type(status) is ClusterStatus, \
            '%s must be a ClusterStatus type' % status

        super(AnsibleClusterAdapter, self).update_status(status)

    def validate(self):
        """
        Adapters may implement this if they need to perform any validation
        steps whenever the cluster info is saved to the database. It should
        return the document with any necessary alterations in the success case,
        or throw an exception if validation fails.
        """
        return self.cluster

    def launch(self):
        # if id is None // Exception
        if self.cluster['status'] >= ClusterStatus.launching:
            raise RestException('Cluster is either already launching, '
                                'or launched.', code=400)

        self.update_status(ClusterStatus.launching)

        base_url = getApiUrl()
        log_write_url = '%s/clusters/%s/log' % (base_url, self.cluster['_id'])
        girder_token = get_task_token()['_id']

        profile, secret_key = _get_profile(self.cluster['profileId'])
        playbook = get_property(
            'config.launch.spec', self.cluster, default=self.DEFAULT_PLAYBOOK)
        playbook_params = get_property(
            'config.launch.params', self.cluster, default={})
        playbook_params['cluster_state'] = 'running'

        cumulus.ansible.tasks.cluster.launch_cluster \
            .delay(playbook, self.cluster, profile, secret_key,
                   playbook_params, girder_token, log_write_url,
                   'launched')

        return self.cluster

    def terminate(self):

        if self.cluster['status'] == ClusterStatus.terminated or \
           self.cluster['status'] == ClusterStatus.terminating:
            return

        self.update_status(ClusterStatus.terminating)

        base_url = getApiUrl()
        log_write_url = '%s/clusters/%s/log' % (base_url, self.cluster['_id'])
        girder_token = get_task_token()['_id']

        profile, secret_key = _get_profile(self.cluster['profileId'])

        playbook = get_property(
            'config.launch.spec', self.cluster, default=self.DEFAULT_PLAYBOOK)
        playbook_params = get_property(
            'config.launch.params', self.cluster, default={})
        playbook_params['cluster_state'] = 'absent'

        cumulus.ansible.tasks.cluster.terminate_cluster \
            .delay(playbook, self.cluster, profile, secret_key,
                   playbook_params, girder_token, log_write_url, 'terminated')

    def provision(self):
        # status must be >= launched.
        self.update_status(ClusterStatus.provisioning)

        base_url = getApiUrl()
        log_write_url = '%s/clusters/%s/log' % (base_url, self.cluster['_id'])
        girder_token = get_task_token()['_id']

        profile, secret_key = _get_profile(self.cluster['profileId'])

        playbook = get_property(
            'config.provision.spec', self.cluster,
            default=self.DEFAULT_PLAYBOOK)
        playbook_params = get_property(
            'config.provision.params', self.cluster, default={})
        provision_ssh_user = get_property(
            'config.provision.ssh.user', self.cluster, default='ubuntu')
        playbook_params['cluster_state'] = 'running'
        playbook_params['ansible_ssh_user'] = provision_ssh_user

        cumulus.ansible.tasks.cluster.provision_cluster \
            .delay(playbook, self.cluster, profile, secret_key,
                   playbook_params,
                   girder_token, log_write_url, 'provisioned')

        return self.cluster

    def start(self, request_body):
        """
        Adapters may implement this if they support a start operation.
        """
        if self.cluster['status'] == 'running':
            raise RestException('Cluster already running.', code=400)

        self.update_status(ClusterStatus.launching)

        self.cluster['config'].setdefault('provision', {})\
            .setdefault('params', {}).update(request_body)
        self.cluster = self.model('cluster', 'cumulus').save(self.cluster)

        base_url = getApiUrl()
        log_write_url = '%s/clusters/%s/log' % (base_url, self.cluster['_id'])
        girder_token = get_task_token()['_id']
        profile, secret_key = _get_profile(self.cluster['profileId'])

        # Launch
        launch_playbook = get_property(
            'config.launch.spec', self.cluster, default=self.DEFAULT_PLAYBOOK)
        launch_playbook_params = get_property(
            'config.launch.params', self.cluster, default={})
        launch_playbook_params['cluster_state'] = 'running'

        # Provision
        provision_playbook = get_property(
            'config.provision.spec', self.cluster, default='gridengine/site')
        provision_playbook_params = get_property(
            'config.provision.params', self.cluster, default={})
        provision_ssh_user = get_property(
            'config.provision.ssh.user', self.cluster, default='ubuntu')
        provision_playbook_params['ansible_ssh_user'] = provision_ssh_user
        provision_playbook_params['cluster_state'] = 'running'

        cumulus.ansible.tasks.cluster.start_cluster \
            .delay(launch_playbook,
                   # provision playbook
                   provision_playbook,
                   self.cluster, profile, secret_key,
                   launch_playbook_params, provision_playbook_params,
                   girder_token, log_write_url)

    def update(self, request_body):
        """
        Adapters may implement this if they support a update operation.
        """
        if 'status' in request_body:
            self.update_status(ClusterStatus[request_body['status']])

    def delete(self):
        """
        Adapters may implement this if they support a delete operation.
        """
        if self.cluster['status'] in [ClusterStatus.running,
                                      ClusterStatus.launching,
                                      ClusterStatus.launched,
                                      ClusterStatus.provisioning,
                                      ClusterStatus.provisioned]:
            raise RestException('Cluster is active', code=400)


def _validate_key(key):
    try:
        parts = key.split()
        key_type, key_string = parts[:2]
        data = base64.b64decode(key_string.encode('utf8'))
        return data[4:11].decode('utf8') == key_type
    except Exception:
        return False


class TraditionClusterAdapter(AbstractClusterAdapter):
    def validate(self):
        query = {
            'name': self.cluster['name'],
            'userId': getCurrentUser()['_id'],
            'type': 'trad'
        }

        if '_id' in self.cluster:
            query['_id'] = {'$ne': self.cluster['_id']}

        duplicate = self.model('cluster', 'cumulus').findOne(query,
                                                             fields=['_id'])
        if duplicate:
            raise ValidationException(
                'A cluster with that name already exists.', 'name')

        return self.cluster

    def update(self, body):

        # Use JSONPath to extract out what we need
        passphrase = parse('config.ssh.passphrase').find(body)
        public_key = parse('config.ssh.publicKey').find(body)

        if passphrase:
            ssh = self.cluster['config'].setdefault('ssh', {})
            ssh['passphrase'] = passphrase[0].value

        if public_key:
            public_key = public_key[0].value
            if not _validate_key(public_key):
                raise RestException('Invalid key format', 400)

            ssh = self.cluster['config'].setdefault('ssh', {})
            ssh['publicKey'] = public_key

        self.cluster = self.model('cluster', 'cumulus').save(self.cluster)

        # Don't return the access object
        del self.cluster['access']
        # Don't return the log
        del self.cluster['log']
        # Don't return the passphrase
        if parse('config.ssh.passphrase').find(self.cluster):
            del self.cluster['config']['ssh']['passphrase']

        return self.cluster

    def start(self, request_body):
        if self.cluster['status'] == 'creating':
            raise RestException('Cluster is not ready to start.', code=400)

        log_write_url = '%s/clusters/%s/log' % (getApiUrl(),
                                                self.cluster['_id'])
        girder_token = get_task_token()['_id']
        cumulus.tasks.cluster.test_connection \
            .delay(self.cluster,
                   log_write_url=log_write_url,
                   girder_token=girder_token)

    def delete(self):
        super(TraditionClusterAdapter, self).delete()
        # Clean up key associate with cluster
        cumulus.ssh.tasks.key.delete_key_pair.delay(self.cluster,
                                                    get_task_token()['_id'])


class NewtClusterAdapter(AbstractClusterAdapter):
    def validate(self):
        query = {
            'name': self.cluster['name'],
            'userId': getCurrentUser()['_id'],
            'type': 'trad'
        }

        if '_id' in self.cluster:
            query['_id'] = {'$ne': self.cluster['_id']}

        duplicate = self.model('cluster', 'cumulus').findOne(query,
                                                             fields=['_id'])
        if duplicate:
            raise ValidationException(
                'A cluster with that name already exists.', 'name')

        return self.cluster

    def update(self, body):

        # Don't return the access object
        del self.cluster['access']
        # Don't return the log
        del self.cluster['log']

        return self.cluster

    def _generate_girder_token(self):
        user = self.model('user').load(self.cluster['userId'], force=True)
        girder_token = self.model('token').createToken(user=user, days=7)

        return girder_token['_id']

    def start(self, request_body):
        log_write_url = '%s/clusters/%s/log' % (getApiUrl(),
                                                self.cluster['_id'])

        girder_token = get_task_token(self.cluster)['_id']
        cumulus.tasks.cluster.test_connection \
            .delay(self.cluster,
                   log_write_url=log_write_url,
                   girder_token=girder_token)

    def submit_job(self, job):
        log_url = '%s/jobs/%s/log' % (getApiUrl(), job['_id'])

        girder_token = get_task_token(self.cluster)['_id']
        cumulus.tasks.job.submit(girder_token, self.cluster, job,
                                 log_url)


type_to_adapter = {
    ClusterType.EC2: AnsibleClusterAdapter,
    ClusterType.ANSIBLE: AnsibleClusterAdapter,
    ClusterType.TRADITIONAL: TraditionClusterAdapter,
    ClusterType.NEWT: NewtClusterAdapter
}


def get_cluster_adapter(cluster):
    global type_to_adapter

    return type_to_adapter[cluster['type']](cluster)
