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

from jsonpath_rw import parse

from celery.canvas import Signature

import cumulus.taskflow
from cumulus.ansible.tasks.providers import CloudProvider

from girder.api.rest import getCurrentUser
from girder.constants import AccessType
from girder.utility.model_importer import ModelImporter

# TODO: rework module file setup?
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

@cumulus.taskflow.task
def setup_cluster(task, *args,**kwargs):
    cluster = kwargs['cluster']

    if '_id' in cluster:
        task.taskflow.logger.info('We are using an existing cluster: %s' % cluster['name'])
    else:
        task.taskflow.logger.info('We are creating an EC2 cluster.')
        task.logger.info('Cluster name %s' % cluster['name'])
        kwargs['machine'] = cluster.get('machine')
        profile = kwargs.get('profile')
        ami = _get_image(task.logger, profile, kwargs['image_spec'])
        cluster = create_ec2_cluster(task, cluster, profile, ami)
        task.logger.info('Cluster started.')

    # Call any follow on task
    if 'next' in kwargs:
        kwargs['cluster'] = cluster
        next = Signature.from_dict(kwargs['next'])
        next.delay(*args, **kwargs)

def create_ec2_cluster(task, cluster, profile, ami):
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
        'master_instance_ami': ami,
        'node_instance_count': nodeCount,
        'node_instance_type': machine_type,
        'node_instance_ami': ami,
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

def _get_image(logger, profile, image_spec):
    # Fetch the image from the CloudProvider
    provider = CloudProvider(profile)
    images = provider.get_machine_images(name=image_spec['name'],
                                         owner=image_spec['owner'])

    if len(images) == 0:
        raise Exception('Unable to locate machine image: %s' % image_spec['name'])
    elif len(images) > 1:
        logger.warn('Found more than one machine image for: %s' % image_spec['name'])

    return images[0]['image_id']