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
from cumulus.tasks.job import terminate_job
from cumulus.constants import JobState

from girder_client import GirderClient, HttpError

from girder.api.rest import getCurrentUser
from girder.constants import AccessType
from girder.utility.model_importer import ModelImporter

CHECKIP_URL = 'http://checkip.amazonaws.com/'


class ClusterProvisioningTaskFlow(cumulus.taskflow.TaskFlow):

    def start(self, *args, **kwargs):
        user = getCurrentUser()
        # Load the cluster
        cluster_id = parse('cluster._id').find(kwargs)
        if cluster_id:
            cluster_id = cluster_id[0].value
            model = ModelImporter.model('cluster', 'cumulus')
            cluster = model.load(cluster_id, user=user, level=AccessType.ADMIN)
            cluster = model.filter(cluster, user, passphrase=False)
            kwargs['cluster'] = cluster

        profile_id = parse('cluster.profileId').find(kwargs)
        if profile_id:
            profile_id = profile_id[0].value
            model = ModelImporter.model('aws', 'cumulus')
            profile = model.load(profile_id, user=user, level=AccessType.ADMIN)
            kwargs['profile'] = profile

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

    if '_id' in cluster:
        task.taskflow.logger.info(
            'We are using an existing cluster: %s' % cluster['name'])
    else:
        task.taskflow.logger.info('We are creating an EC2 cluster.')
        task.logger.info('Cluster name %s' % cluster['name'])
        kwargs['machine'] = cluster.get('machine')
        profile = kwargs.get('profile')
        cluster = create_ec2_cluster(
            task, cluster, profile, kwargs['image_spec'])
        task.logger.info('Cluster started.')

    # Call any follow on task
    if 'next' in kwargs:
        kwargs['cluster'] = cluster
        next = Signature.from_dict(kwargs['next'])
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
    nodeCount = cluster['clusterSize']-1
    launch_spec = 'ec2'

    # Look up the external IP of the deployment to user for firewall rules
    r = requests.get(CHECKIP_URL)
    r.raise_for_status()
    source_ip = '%s/32' % r.text.strip()

    extra_rules = [{
        'proto': 'tcp',
        'from_port': 9000,
        'to_port': 9000,
        'cidr_ip': source_ip
    }]

    task.logger.info('Using source ip: %s' % source_ip)

    launch_params = {
        'master_instance_type': machine_type,
        'master_ami_spec': ami_spec,
        'node_instance_count': nodeCount,
        'node_instance_type': machine_type,
        'node_ami_spec': ami_spec,
        'gpu': cluster['machine']['gpu'],
        'source_cidr_ip': source_ip,
        'extra_rules': extra_rules
    }
    provision_spec = 'gridengine/site'
    provision_params = {
        'ansible_ssh_user': 'ubuntu'
    }

    body = {
        'type': 'ec2',
        'name': cluster['name'],
        'config': {
            'launch': {
                'spec': launch_spec,
                'params': launch_params
            },
            'provision': {
                'spec': provision_spec
            }
        },
        'profileId': cluster['profileId']
    }
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    try:
        cluster = client.post('clusters',  data=json.dumps(body))
    except HttpError as he:
        task.logger.exception(he.responseText)
        raise

    msg = 'Created cluster: %s' % cluster['_id']
    task.taskflow.logger.info(msg)
    task.logger.info(msg)

    # Now save cluster id in metadata
    task.taskflow.set_metadata('cluster', cluster)

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
    provision_params['cluster_state'] = 'running'
    launch_params['cluster_state'] = 'running'
    girder_token = task.taskflow.girder_token
    cumulus.ansible.tasks.cluster.start_cluster(
        launch_spec, provision_spec, cluster, profile, secret_key,
        launch_params, provision_params, girder_token, log_write_url)

    # Get the update to date cluster
    cluster = client.get('clusters/%s' % cluster['_id'])

    return cluster


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