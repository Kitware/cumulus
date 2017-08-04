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

from cumulus.celery import command
from cumulus.common import check_status
from cumulus.ansible.tasks.providers import CloudProvider
from .inventory import simple_inventory
import cumulus
import requests
import os
from celery.utils.log import get_task_logger

from cumulus.ansible.tasks.utils import get_playbook_path
from cumulus.ansible.tasks.utils import check_girder_cluster_status
from cumulus.ansible.tasks.utils import get_callback_plugins_path
from cumulus.ansible.tasks.utils import get_playbook_variables
from cumulus.ansible.tasks.utils import run_playbook
from cumulus.ansible.tasks.utils import check_ansible_return_code
from cumulus.ansible.tasks.volume import detach_volume

logger = get_task_logger(__name__)


@command.task
def provision_cluster(playbook, cluster, profile, secret_key, extra_vars,
                      girder_token, log_write_url, post_status):

    playbook = get_playbook_path(playbook)
    playbook_variables = get_playbook_variables(cluster, profile, extra_vars)

    env = os.environ.copy()
    env.update({'AWS_ACCESS_KEY_ID': profile['accessKeyId'],
                'AWS_SECRET_ACCESS_KEY': secret_key,
                'GIRDER_TOKEN': girder_token,
                'LOG_WRITE_URL': log_write_url,
                'CLUSTER_ID': cluster['_id'],
                'REGION_NAME': profile['regionName'],
                'ANSIBLE_HOST_KEY_CHECKING': 'false',
                'ANSIBLE_CALLBACK_PLUGINS': get_callback_plugins_path()})

    inventory = os.path.join(os.path.dirname(__file__), 'providers', 'ec2.py')

    ansible = run_playbook(playbook, inventory, playbook_variables,
                           env=env, verbose=3)

    check_girder_cluster_status(cluster, girder_token, post_status)
    check_ansible_return_code(ansible, cluster, girder_token)


@command.task
def start_cluster(launch_playbook, provision_playbook, cluster, profile,
                  secret_key, launch_extra_vars, provision_extra_vars,
                  girder_token, log_write_url):

    launch_cluster(launch_playbook, cluster, profile, secret_key,
                   launch_extra_vars, girder_token, log_write_url, 'running')

    check_girder_cluster_status(cluster, girder_token, 'provisioning')

    provision_cluster(provision_playbook, cluster, profile, secret_key,
                      provision_extra_vars, girder_token, log_write_url,
                      'running')

    # Now update the cluster state to 'running'
    check_girder_cluster_status(cluster, girder_token, 'running')


@command.task
def launch_cluster(playbook, cluster, profile, secret_key, extra_vars,
                   girder_token, log_write_url, post_status):
    playbook = get_playbook_path(playbook)
    playbook_variables = get_playbook_variables(cluster, profile, extra_vars)

    env = os.environ.copy()
    env.update({'AWS_ACCESS_KEY_ID': profile['accessKeyId'],
                'AWS_SECRET_ACCESS_KEY': secret_key,
                'GIRDER_TOKEN': girder_token,
                'LOG_WRITE_URL': log_write_url,
                'CLUSTER_ID': cluster['_id']})

    inventory = simple_inventory('localhost')

    with inventory.to_tempfile() as inventory_path:
        ansible = run_playbook(playbook, inventory_path, playbook_variables,
                               env=env, verbose=3)

    p = CloudProvider(dict(secretAccessKey=secret_key, **profile))

    master = p.get_master_instance(cluster['_id'])

    status_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl,
                                     cluster['_id'])
    updates = {
        'config': {
            'host': master['public_ip']
        }
    }
    headers = {'Girder-Token': girder_token}
    r = requests.patch(status_url, headers=headers, json=updates)
    check_status(r)

    check_ansible_return_code(ansible, cluster, girder_token)
    check_girder_cluster_status(cluster, girder_token, post_status)


@command.task
def terminate_cluster(playbook, cluster, profile, secret_key, extra_vars,
                      girder_token, log_write_url, post_status):

    playbook = get_playbook_path(playbook)
    playbook_variables = get_playbook_variables(cluster, profile, extra_vars)

    env = os.environ.copy()
    env.update({'AWS_ACCESS_KEY_ID': profile['accessKeyId'],
                'AWS_SECRET_ACCESS_KEY': secret_key,
                'GIRDER_TOKEN': girder_token,
                'LOG_WRITE_URL': log_write_url,
                'CLUSTER_ID': cluster['_id']})

    # if there are any volumes,  make sure to detach them first.
    if 'volumes' in cluster and len(cluster['volumes']):
        p = CloudProvider(dict(secretAccessKey=secret_key, **profile))
        master = p.get_master_instance(cluster['_id'])

        for volume_id in cluster['volumes']:
            r = requests.get('%s/volumes/%s' %
                             (cumulus.config.girder.baseUrl, volume_id),
                             headers={'Girder-Token': girder_token})
            check_status(r)
            volume = r.json()

            girder_callback_info = {
                'girder_api_url': cumulus.config.girder.baseUrl,
                'girder_token': girder_token}

            vol_log_url = '%s/volumes/%s/log' % (cumulus.config.girder.baseUrl,
                                                 volume_id)
            detach_volume(profile, cluster, master, volume,
                          secret_key, vol_log_url, girder_callback_info)

    inventory = simple_inventory('localhost')

    with inventory.to_tempfile() as inventory_path:
        ansible = run_playbook(playbook, inventory_path, playbook_variables,
                               env=env, verbose=3)

    check_ansible_return_code(ansible, cluster, girder_token)
    check_girder_cluster_status(cluster, girder_token, post_status)
