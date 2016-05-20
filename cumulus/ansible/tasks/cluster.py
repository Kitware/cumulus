from cumulus.celery import command
from cumulus.common import check_status
from cumulus.ansible.tasks.providers import Provider
from inventory import simple_inventory
import cumulus
import requests
import os
from celery.utils.log import get_task_logger

from cumulus.ansible.tasks.utils import get_playbook_path
from cumulus.ansible.tasks.utils import check_girder_cluster_status
from cumulus.ansible.tasks.utils import provision_cluster
from cumulus.ansible.tasks.utils import get_playbook_variables
from cumulus.ansible.tasks.utils import run_playbook
from cumulus.ansible.tasks.utils import check_ansible_return_code

logger = get_task_logger(__name__)


@command.task
def start_cluster(launch_playbook, provision_playbook, cluster, profile,
                  secret_key, launch_extra_vars, provision_extra_vars,
                  girder_token, log_write_url):

    launch_cluster(launch_playbook, cluster, profile, secret_key,
                   launch_extra_vars, girder_token, log_write_url, 'launched')

    # todo cluster statuses should be updated in celery tasks?
    check_girder_cluster_status(cluster, girder_token, 'provisioning')

    provision_cluster(provision_playbook, cluster, profile, secret_key,
                      provision_extra_vars, girder_token, log_write_url,
                      'provisioned')

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

    p = Provider(dict(secretAccessKey=secret_key, **profile))

    master = p.get_master_instance(cluster)

    status_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl,
                                     cluster['_id'])
    updates = {
        'config': {
            'host': master['public_ip']
        }
    }
    headers = {'Girder-Token':  girder_token}
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

    inventory = simple_inventory('localhost')

    with inventory.to_tempfile() as inventory_path:
        ansible = run_playbook(playbook, inventory_path, playbook_variables,
                               env=env, verbose=3)

    check_ansible_return_code(ansible, cluster, girder_token)
    check_girder_cluster_status(cluster, girder_token, post_status)
