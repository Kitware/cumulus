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

import json
from jsonpath_rw import parse
import requests

from celery.canvas import Signature

import cumulus.taskflow
import cumulus.ansible.tasks.volume
from cumulus.ansible.tasks.providers import CloudProvider, InstanceState
from cumulus.tasks.job import terminate_job
from cumulus.constants import JobState
from cumulus.ansible.tasks.utils import check_girder_cluster_status

from girder_client import GirderClient, HttpError

from girder.api.rest import getCurrentUser
from girder.constants import AccessType

CHECKIP_URL = 'http://checkip.amazonaws.com/'
PROVISION_SPEC = 'gridengine/site'


class ClusterProvisioningTaskFlow(cumulus.taskflow.TaskFlow):

    def start(self, *args, **kwargs):

        # Import the required models
        # If these are registered elsewhere, we may be able to use
        # the ModelImporter instead.
        from cumulus_plugin.models.aws import Aws as AwsModel
        from cumulus_plugin.models.cluster import Cluster as ClusterModel
        from cumulus_plugin.models.volume import Volume as VolumeModel

        user = getCurrentUser()
        # Load the cluster
        cluster_id = parse('cluster._id').find(kwargs)
        if cluster_id:
            cluster_id = cluster_id[0].value
            model = ClusterModel()
            cluster = model.load(cluster_id, user=user, level=AccessType.ADMIN)
            cluster = model.filter(cluster, user, passphrase=False)
            kwargs['cluster'] = cluster

        profile_id = parse('cluster.profileId').find(kwargs)
        if profile_id:
            profile_id = profile_id[0].value
            model = AwsModel()
            profile = model.load(profile_id, user=user, level=AccessType.ADMIN)
            kwargs['profile'] = profile

        volume_id = parse('volume._id').find(kwargs)
        if volume_id:
            volume_id = volume_id[0].value
            model = VolumeModel()
            volume = model.load(volume_id, user=user, level=AccessType.ADMIN)
            kwargs['volume'] = volume

        super(ClusterProvisioningTaskFlow, self).start(
            setup_cluster.s(self, *args, **kwargs))

    def terminate(self):
        self.run_task(job_terminate.s())

    def delete(self):
        for job in self.get('meta', {}).get('jobs', []):
            job_id = job['_id']
            client = create_girder_client(self.girder_api_url,
                                          self.girder_token)
            client.delete('jobs/%s' % job_id)

            try:
                client.get('jobs/%s' % job_id)
            except HttpError as e:
                if e.status != 404:
                    self.logger.error('Unable to delete job: %s' % job_id)


@cumulus.taskflow.task
def setup_cluster(task, *args, **kwargs):
    cluster = kwargs['cluster']
    profile = kwargs.get('profile')
    volume = kwargs.get('volume')
    new = False

    if '_id' in cluster:
        task.taskflow.logger.info(
            'We are using an existing cluster: %s' % cluster['name'])
    else:
        new = True
        task.taskflow.logger.info('We are creating an EC2 cluster.')
        task.logger.info('Cluster name %s' % cluster['name'])
        kwargs['machine'] = cluster.get('machine')

        if volume:
            config = cluster.setdefault('config', {})
            config['jobOutputDir'] = '/data'

        # Create the model in Girder
        cluster = create_ec2_cluster(task, cluster, profile,
                                     kwargs['image_spec'])

        # Now launch the cluster
        cluster = launch_ec2_cluster(task, cluster, profile)

        task.logger.info('Cluster started.')

    if volume and '_id' in volume:
        task.taskflow.logger.info(
            'We are using an existing volume: %s' % volume['name'])
    elif volume:
        task.taskflow.logger.info('We are creating a new volume: "%s"' %
                                  volume['name'])
        volume = create_volume(task, volume, profile)

    # Now provision
    if new:
        provision_params = {}

        girder_token = task.taskflow.girder_token
        check_girder_cluster_status(cluster, girder_token, 'provisioning')

        # attach volume
        if volume:
            volume = _attach_volume(task, profile, volume, cluster)
            path = volume.get('path')
            if path:
                provision_params['master_nfs_exports_extra'] = [path]

        cluster = provision_ec2_cluster(task, cluster, profile,
                                        provision_params)

    # Call any follow on task
    if 'next' in kwargs:
        kwargs['cluster'] = cluster
        next = Signature.from_dict(kwargs['next'])

        if next.task == 'celery.chain':
            # If we are dealing with a chain we want to update the arg and
            # kwargs passed into the chain.
            first_task = next.kwargs['tasks'][0]
            if first_task:
                if args:
                    first_task.args = tuple(args) + tuple(first_task.args)

                if kwargs:
                    first_task.kwargs = dict(first_task.kwargs, **kwargs)

        next.delay(*args, **kwargs)


@cumulus.taskflow.task
def job_terminate(task):
    cluster = parse('meta.cluster').find(task.taskflow)
    if cluster:
        cluster = cluster[0].value
    else:
        task.logger.warning('Unable to extract cluster from taskflow. '
                            'Unable to terminate job.')

    client = create_girder_client(task.taskflow.girder_api_url,
                                  task.taskflow.girder_token)

    jobs = task.taskflow.get('meta', {}).get('jobs', [])
    terminate_jobs(task, client, cluster, jobs)


def create_ec2_cluster(task, cluster, profile, ami_spec):
    machine_type = cluster['machine']['id']
    nodeCount = cluster['clusterSize'] - 1
    launch_spec = 'ec2'

    # Look up the external IP of the deployment to user for firewall rules
    r = requests.get(CHECKIP_URL)
    r.raise_for_status()
    source_ip = '%s/32' % r.text.strip()

    ec2_pod_rules = [
        {
            'proto': 'tcp',
            'from_port': 22,
            'to_port': 22,
            'cidr_ip': '0.0.0.0/0'
        },
        {
            'proto': 'tcp',
            'from_port': 9000,
            'to_port': 9000,
            'cidr_ip': source_ip
        },
        # Clean this up when we are able to pass in the group_name.
        {
            'proto': 'all',
            'group_name': 'ec2_pod_{{cluster_id}}',
            'group_desc': 'Ec2 security group for cluster {{ cluster_id }}'
        }
    ]

    task.logger.info('Using source ip: %s' % source_ip)

    launch_params = {
        'master_instance_type': machine_type,
        'master_ami_spec': ami_spec,
        'node_instance_count': nodeCount,
        'node_instance_type': machine_type,
        'node_ami_spec': ami_spec,
        'gpu': cluster['machine']['gpu'],
        'source_cidr_ip': source_ip,
        'ec2_pod_rules': ec2_pod_rules
    }

    body = {
        'type': 'ec2',
        'name': cluster['name'],
        'config': dict(cluster.get('config', {}), **{
            'launch': {
                'spec': launch_spec,
                'params': launch_params
            },
            'provision': {
                'spec': PROVISION_SPEC
            }
        }),
        'profileId': cluster['profileId']
    }
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    try:
        cluster = client.post('clusters', data=json.dumps(body))
    except HttpError as he:
        task.logger.exception(he.responseText)
        raise

    msg = 'Created cluster: %s' % cluster['_id']
    task.taskflow.logger.info(msg)
    task.logger.info(msg)

    # Now save cluster id in metadata
    task.taskflow.set_metadata('cluster', cluster)

    return cluster


def launch_ec2_cluster(task, cluster, profile):
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    launch_spec = parse('config.launch.spec').find(cluster)[0].value
    launch_params = parse('config.launch.params').find(cluster)[0].value

    task.logger.info('Starting cluster.')

    body = {
        'status': 'launching'
    }
    client.patch('clusters/%s' % cluster['_id'], data=json.dumps(body))

    secret_key = profile['secretAccessKey']
    profile = profile.copy()
    del profile['secretAccessKey']
    log_write_url = '%s/clusters/%s/log' % (task.taskflow.girder_api_url,
                                            cluster['_id'])
    girder_token = task.taskflow.girder_token
    launch_params['cluster_state'] = 'running'
    cumulus.ansible.tasks.cluster.launch_cluster(
        launch_spec, cluster, profile, secret_key,
        launch_params, girder_token, log_write_url, 'running')

    # Get the update to data cluster
    cluster = client.get('clusters/%s' % cluster['_id'])

    return cluster


def provision_ec2_cluster(task, cluster, profile, provision_params={}):

    if 'ansible_ssh_user' not in provision_params:
        provision_params['ansible_ssh_user'] = 'ubuntu'

    if 'cluster_state' not in provision_params:
        provision_params['cluster_state'] = 'running'

    girder_token = task.taskflow.girder_token
    client = create_girder_client(
        task.taskflow.girder_api_url, girder_token)
    secret_key = profile['secretAccessKey']
    profile = profile.copy()
    del profile['secretAccessKey']
    log_write_url = '%s/clusters/%s/log' % (task.taskflow.girder_api_url,
                                            cluster['_id'])

    check_girder_cluster_status(cluster, girder_token, 'provisioning')

    cumulus.ansible.tasks.cluster.provision_cluster(
        PROVISION_SPEC, cluster, profile, secret_key, provision_params,
        girder_token, log_write_url, 'running')

    # Now update the cluster state to 'running' Is this needed?
    check_girder_cluster_status(cluster, girder_token, 'running')

    # Get the update to data cluster
    cluster = client.get('clusters/%s' % cluster['_id'])

    return cluster


def create_volume(task, volume, profile):
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    body = {
        'name': volume['name'],
        'size': volume['size'],
        'type': 'ebs',
        'zone': profile['availabilityZone'],
        'profileId': profile['_id']
    }

    # create volume model
    try:
        volume = client.post('volumes', data=json.dumps(body))
    except HttpError as he:
        task.logger.exception(he.responseText)
        raise

    msg = 'Created volume: %s' % volume['_id']
    task.taskflow.logger.info(msg)
    task.logger.info(msg)

    return volume


def _attach_volume(task, profile, volume, cluster):
    girder_callback_info = {
        'girder_api_url': task.taskflow.girder_api_url,
        'girder_token': task.taskflow.girder_token}
    p = CloudProvider(dict(**profile))
    master = p.get_master_instance(cluster['_id'])
    if master['state'] != InstanceState.RUNNING:
        task.logger.exception('Master instance is not running!')
        raise
    log_write_url = '%s/volumes/%s/log' % (task.taskflow.girder_api_url,
                                           volume['_id'])
    cumulus.ansible.tasks.volume.attach_volume(
        profile, cluster, master,
        volume, '/data', profile['secretAccessKey'],
        log_write_url, girder_callback_info)
    task.logger.info('Volume attached.')

    # Get the up to date volume
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)
    volume = client.get('volumes/%s' % volume['_id'])

    return volume


def create_girder_client(girder_api_url, girder_token):
    client = GirderClient(apiUrl=girder_api_url)
    client.token = girder_token

    return client


def terminate_jobs(task, client, cluster, jobs):
    for job in jobs:
        task.logger.info('Terminating job %s' % job['_id'])
        # Fetch the latest job info
        job_url = 'jobs/%s' % job['_id']
        job = client.get(job_url)

        # Update the status to terminating
        body = {
            'status': JobState.TERMINATING
        }
        client.patch(job_url, data=json.dumps(body))

        terminate_job(
            cluster, job, log_write_url=None,
            girder_token=task.taskflow.girder_token)
