from cumulus.celery import command
from cumulus.common import check_status
from cumulus.ansible.tasks.dynamic_inventory.ec2 import get_inventory
from inventory import AnsibleInventory
import cumulus
import requests
import os
import json
import subprocess
from celery.utils.log import get_task_logger
import select
from jsonpath_rw import parse
import pkg_resources as pr

logger = get_task_logger(__name__)


def get_playbook_directory():
    return pr.resource_filename('cumulus', 'ansible/tasks/playbooks')


def get_playbook_path(name):
    return os.path.join(get_playbook_directory(), name + '.yml')


def get_callback_plugins_path():
    return os.path.join(get_playbook_directory(),
                        'callback_plugins')


def run_playbook(playbook, inventory, extra_vars=None,
                 verbose=None, env=None):

    env = env if env is not None else os.environ.copy()

    cmd = ['ansible-playbook', '-i', inventory]

    if verbose is not None:
        cmd.append('-%s' % ('v' * verbose))

    if extra_vars is not None:
        cmd.extend(['--extra-vars', json.dumps(extra_vars)])

    cmd.append(playbook)

    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    while True:
        reads = [p.stdout.fileno(), p.stderr.fileno()]
        ret = select.select(reads, [], [])

        for fd in ret[0]:
            if fd == p.stdout.fileno():
                logger.info(p.stdout.readline())
            if fd == p.stderr.fileno():
                logger.error(p.stderr.readline())

        if p.poll() is not None:
            return p.wait()


def get_playbook_variables(cluster, profile, extra_vars):
    # Default variables all playbooks will need
    playbook_variables = {
        'cluster_region': profile['regionName'],
        'cluster_id': cluster['_id']
    }

    # Update with variables passed in from the cluster adapater
    playbook_variables.update(extra_vars)

    # If no keyname is provided use the one associated with the profile
    if 'aws_keyname' not in playbook_variables:
        playbook_variables['aws_keyname'] = profile['_id']

    return playbook_variables


def check_girder_cluster_status(cluster, girder_token, post_status):
    # Check status from girder
    cluster_id = cluster['_id']
    headers = {'Girder-Token':  girder_token}
    status_url = '%s/clusters/%s/status' % (cumulus.config.girder.baseUrl,
                                            cluster_id)
    r = requests.get(status_url, headers=headers)
    status = r.json()['status']

    if status != 'error':
        # Update girder with the new status
        status_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl,
                                         cluster_id)
        updates = {
            'status': post_status
        }

        r = requests.patch(status_url, headers=headers, json=updates)
        check_status(r)


def check_ansible_return_code(returncode, cluster, girder_token):
    if returncode != 0:
        check_status(requests.patch('%s/clusters/%s' %
                                    (cumulus.config.girder.baseUrl,
                                     cluster['_id']),
                                    headers={'Girder-Token': girder_token},
                                    json={'status': 'error'}))


@command.task
def provision_cluster(playbook, cluster, profile, secret_key, extra_vars,
                      girder_token, log_write_url, post_status):
    from cumulus.ssh.tasks.key import _key_path

    playbook = get_playbook_path(playbook)
    playbook_variables = get_playbook_variables(cluster, profile, extra_vars)

    env = os.environ.copy()
    env.update({'AWS_ACCESS_KEY_ID': profile['accessKeyId'],
                'AWS_SECRET_ACCESS_KEY': secret_key,
                'GIRDER_TOKEN': girder_token,
                'LOG_WRITE_URL': log_write_url,
                'CLUSTER_ID': cluster['_id'],
                'ANSIBLE_HOST_KEY_CHECKING': 'false',
                'ANSIBLE_CALLBACK_PLUGINS': get_callback_plugins_path(),
                'PRIVATE_KEY_FILE': _key_path(profile)})

    inventory = os.path.join(os.path.dirname(__file__), 'dynamic_inventory')

    ansible = run_playbook(playbook, inventory, playbook_variables,
                           env=env, verbose=3)

    check_ansible_return_code(ansible, cluster, girder_token)
    check_girder_cluster_status(cluster, girder_token, post_status)


@command.task
def start_cluster(launch_playbook, provision_playbook, cluster, profile,
                  secret_key, launch_extra_vars, provision_extra_vars,
                  girder_token, log_write_url, master_name=None):

    run_ansible(launch_playbook, cluster, profile, secret_key,
                launch_extra_vars, girder_token, log_write_url, 'launched')

    # todo cluster statuses should be updated in celery tasks?
    check_girder_cluster_status(cluster, girder_token, 'provisioning')

    provision_cluster(provision_playbook, cluster, profile, secret_key,
                      provision_extra_vars, girder_token, log_write_url,
                      'provisioned')

    # Update the hostname for the cluster
    inventory = get_inventory(
        aws_access_key_id=profile['accessKeyId'],
        aws_secret_access_key=secret_key,
        cluster_id=cluster['_id'])

    # If master_name has been provide extract out the master host and
    # update the cluster with it.
    if master_name:
        master = parse('%s.hosts[0]' % master_name).find(inventory)
        if master:
            master = master[0].value
        else:
            raise Exception('Unable to extract cluster master host.')

        status_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl,
                                         cluster['_id'])
        updates = {
            'config': {
                'host': master
            }
        }
        headers = {'Girder-Token':  girder_token}
        r = requests.patch(status_url, headers=headers, json=updates)
        check_status(r)

    # Now update the cluster state to 'running'
    check_girder_cluster_status(cluster, girder_token, 'running')


@command.task
def run_ansible(playbook, cluster, profile, secret_key, extra_vars,
                girder_token, log_write_url, post_status):
    playbook = get_playbook_path(playbook)
    playbook_variables = get_playbook_variables(cluster, profile, extra_vars)

    env = os.environ.copy()
    env.update({'AWS_ACCESS_KEY_ID': profile['accessKeyId'],
                'AWS_SECRET_ACCESS_KEY': secret_key,
                'GIRDER_TOKEN': girder_token,
                'LOG_WRITE_URL': log_write_url,
                'CLUSTER_ID': cluster['_id']})

    inventory = AnsibleInventory(['localhost'])

    with inventory.to_tempfile() as inventory_path:
        ansible = run_playbook(playbook, inventory_path, playbook_variables,
                               env=env, verbose=3)

    check_ansible_return_code(ansible, cluster, girder_token)
    check_girder_cluster_status(cluster, girder_token, post_status)