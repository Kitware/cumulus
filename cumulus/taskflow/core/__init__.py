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

import cumulus.taskflow
from cumulus.ansible.tasks.providers import CloudProvider

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