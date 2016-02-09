from cumulus.celery import command
import ansible.playbook
from ansible import callbacks
from cumulus.common import check_status
import cumulus
import requests
import os

DEFAULT_PLAYBOOK = os.path.join(os.path.dirname(__file__),
                                "playbooks/default.yml")

@command.task
def launch_cluster(cluster, profile, secret_key, girder_token, log_write_url):
    stats = callbacks.AggregateStats()

    extra_vars = {
        "girder_token": girder_token,
        "log_write_url": log_write_url,
        "cluster_region": profile['regionName'],
        "cluster_state": "running",
        "cluster_id": cluster["_id"],
        "aws_access_key": profile['accessKeyId'],
        "aws_secret_key": secret_key
    }

    extra_vars.update(cluster.get('template', {}))

    pb = ansible.playbook.PlayBook(
        playbook=DEFAULT_PLAYBOOK,
        inventory=ansible.inventory.Inventory(['localhost']),
        callbacks=callbacks.PlaybookCallbacks(verbose=1),
        runner_callbacks=callbacks.PlaybookRunnerCallbacks(stats, verbose=1),
        stats=stats,
        extra_vars=extra_vars
    )

    # Note:  can refer to callback.playbook.extra_vars  to get access
    # to girder_token after this point

    pb.run()

    cluster_id = cluster['_id']
    status_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl, cluster_id)
    headers = {'Girder-Token':  girder_token}
    updates = {
        "status": "launched"
    }

    r = requests.patch(status_url, headers=headers, json=updates)
    check_status(r)


@command.task
def terminate_cluster(cluster, profile, secret_key,
                      girder_token, log_write_url):
    stats = callbacks.AggregateStats()

    extra_vars = {
        "girder_token": girder_token,
        "log_write_url": log_write_url,
        "cluster_region": profile['regionName'],
        "cluster_state": "absent",
        "cluster_id": cluster["_id"],
        "aws_access_key": profile['accessKeyId'],
        "aws_secret_key": secret_key
    }

    extra_vars.update(cluster.get('template', {}))

    pb = ansible.playbook.PlayBook(
        playbook=DEFAULT_PLAYBOOK,
        inventory=ansible.inventory.Inventory(['localhost']),
        callbacks=callbacks.PlaybookCallbacks(verbose=1),
        runner_callbacks=callbacks.PlaybookRunnerCallbacks(stats, verbose=1),
        stats=stats,
        extra_vars=extra_vars
    )

    # Note:  can refer to callback.playbook.extra_vars  to get access
    # to girder_token after this point

    pb.run()
