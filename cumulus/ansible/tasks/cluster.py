from cumulus.celery import command
import ansible.playbook
from ansible import callbacks
from cumulus.common import check_status
import cumulus
import requests
import os


def get_playbook_path(name):
    return os.path.join(os.path.dirname(__file__),
                        "playbooks/" + name + ".yml")


def run_playbook(playbook, inventory, extra_vars=None):
    extra_vars = {} if extra_vars is None else extra_vars

    stats = callbacks.AggregateStats()

    pb = ansible.playbook.PlayBook(
        playbook=playbook,
        inventory=inventory,
        callbacks=callbacks.PlaybookCallbacks(verbose=1),
        runner_callbacks=callbacks.PlaybookRunnerCallbacks(stats, verbose=1),
        stats=stats,
        extra_vars=extra_vars
    )
    # Note:  can refer to callback.playbook.extra_vars  to get access
    # to girder_token after this point
    results = pb.run()

    return results


@command.task
def run_ansible(cluster, profile, secret_key, extra_vars,
                girder_token, log_write_url, post_status):

    playbook = get_playbook_path(cluster.get("playbook", "default"))

    inventory = ansible.inventory.Inventory(['localhost'])

    # Default variables all playbooks will need
    playbook_variables = {
        "girder_token": girder_token,
        "log_write_url": log_write_url,
        "cluster_region": profile['regionName'],
        "cluster_id": cluster["_id"],
        "aws_access_key": profile['accessKeyId'],
        "aws_secret_key": secret_key
    }

    # Update with variables passed in from the cluster adapater
    playbook_variables.update(extra_vars)

    # Update with variables passed in as apart of the cluster configuration
    playbook_variables.update(cluster.get('playbook_variables', {}))

    # If no keyname is provided use the one associated with the profile
    if 'aws_keyname' not in playbook_variables:
        playbook_variables['aws_keyname'] = profile['_id']

    # Run the playbook
    run_playbook(playbook, inventory, playbook_variables)

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
