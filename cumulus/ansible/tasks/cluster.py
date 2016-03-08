from cumulus.celery import command
from cumulus.common import check_status
from inventory import AnsibleInventory
import cumulus
import requests
import os
import json
import subprocess
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def get_playbook_path(name):
    return os.path.join(os.path.dirname(__file__),
                        "playbooks/" + name + ".yml")

def run_playbook(playbook, inventory, extra_vars=None,
                 verbose=None, env=None):

    env = env if env is not None else os.environ.copy()

    cmd = ["ansible-playbook", "-i", inventory]

    if verbose is not None:
        cmd.append("-%s" % ("v" * verbose))

    if extra_vars is not None:
        cmd.extend(["--extra-vars", json.dumps(extra_vars)])

    cmd.append(playbook)

    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE)

    for line in iter(p.stdout.readline, b''):
        logger.info(line)


def get_playbook_variables(cluster, profile, extra_vars):
    # Default variables all playbooks will need
    playbook_variables = {
        "cluster_region": profile['regionName'],
        "cluster_id": cluster["_id"]
    }

    # Update with variables passed in from the cluster adapater
    playbook_variables.update(extra_vars)

    # Update with variables passed in as apart of the cluster configuration
    playbook_variables.update(cluster.get('cluster_config', {}))

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
            "status": post_status
        }

        r = requests.patch(status_url, headers=headers, json=updates)
        check_status(r)


@command.task
def provision_cluster(playbook, cluster, profile, secret_key, extra_vars,
                      girder_token, log_write_url, post_status):
    playbook = get_playbook_path(playbook)
    playbook_variables = get_playbook_variables(cluster, profile, extra_vars)

    env = os.environ.copy()
    env.update({"AWS_ACCESS_KEY_ID": profile['accessKeyId'],
                "AWS_SECRET_ACCESS_KEY": secret_key,
                "GIRDER_TOKEN": girder_token,
                "LOG_WRITE_URL": log_write_url,
                "CLUSTER_ID": cluster["_id"]})

    inventory = os.path.join(os.path.dirname(__file__), 'dynamic-inventory')

    run_playbook(playbook, inventory, playbook_variables,
                 env=env, verbose=3)

    check_girder_cluster_status(cluster, girder_token, post_status)


@command.task
def run_ansible(playbook, cluster, profile, secret_key, extra_vars,
                girder_token, log_write_url, post_status):
    playbook = get_playbook_path(playbook)
    playbook_variables = get_playbook_variables(cluster, profile, extra_vars)

    env = os.environ.copy()
    env.update({"AWS_ACCESS_KEY_ID": profile['accessKeyId'],
                "AWS_SECRET_ACCESS_KEY": secret_key,
                "GIRDER_TOKEN": girder_token,
                "LOG_WRITE_URL": log_write_url,
                "CLUSTER_ID": cluster["_id"]})

    inventory = AnsibleInventory(["localhost"])

    with inventory.to_tempfile() as inventory_path:
        run_playbook(playbook, inventory_path, playbook_variables,
                     env=env, verbose=3)

    check_girder_cluster_status(cluster, girder_token, post_status)
