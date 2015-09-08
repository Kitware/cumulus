import requests

import cumulus.starcluster.logging
from cumulus.starcluster.tasks.common import _check_status, get_ssh_connection
from cumulus.starcluster.tasks.celery import command
import starcluster.logger


@command.task
@cumulus.starcluster.logging.capture
def test_connection(cluster, log_write_url=None, girder_token=None):
    cluster_id = cluster['_id']
    status_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl, cluster_id)
    log = starcluster.logger.get_starcluster_logger()
    headers = {'Girder-Token':  girder_token}

    try:
        ssh = get_ssh_connection(girder_token, cluster)
        status = 'running'
        # For simply test can we can connect to cluster and qsub is installed
        output = ssh.execute('command -v qsub')
        if len(output) < 1:
            log.error('Unable to find qsub on cluster')
            status = 'error'

        r = requests.patch(
            status_url, headers=headers, json={'status': status})
        _check_status(r)
    except Exception as ex:
        r = requests.patch(status_url, headers=headers,
                           json={'status': 'error'})
        # Log the error message
        log.exception(ex)
