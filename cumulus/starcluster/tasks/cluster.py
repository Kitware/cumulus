from cumulus.starcluster.logging import logstdout
import cumulus.starcluster.logging
from cumulus.starcluster.tasks.common import _write_config_file, _check_status
from cumulus.starcluster.tasks.job import submit
from cumulus.starcluster.tasks.celery import command
import starcluster.config
import starcluster.logger
import starcluster.exception
import requests
import os
import time


@command.task
@cumulus.starcluster.logging.capture
def start_cluster(cluster, log_write_url=None, on_start_submit=None,
                  girder_token=None):
    config_filepath = None
    name = cluster['name']
    template = cluster['template']
    cluster_id = cluster['_id']
    config_id = cluster['config']['_id']
    config_url = '%s/starcluster-configs/%s' % (
        cumulus.config.girder.baseUrl, config_id)
    status_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl, cluster_id)
    log = starcluster.logger.get_starcluster_logger()

    try:

        config_filepath = _write_config_file(girder_token, config_url)
        headers = {'Girder-Token':  girder_token}
        r = requests.patch(
            status_url, headers=headers, json={'status': 'initializing'})
        _check_status(r)

        config = starcluster.config.StarClusterConfig(config_filepath)

        config.load()
        sc = config.get_cluster_template(template, name)
        sc.refresh_interval = 5

        start = time.time()

        with logstdout():
            sc.start()

        end = time.time()

        startup_time = end - start
        updates = {
            'timings': {
                'startup': int(round(startup_time * 1000))
            },
            'status': 'running'
        }

        # Now update the status of the cluster
        r = requests.patch(status_url, headers=headers, json=updates)
        _check_status(r)

        # Now if we have a job to submit do it!
        if on_start_submit:
            job_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl,
                                      on_start_submit)

            # Get the Job information
            r = requests.get(job_url, headers=headers)
            _check_status(r)
            job = r.json()
            log_url = '%s/jobs/%s/log' % (cumulus.config.girder.baseUrl,
                                          on_start_submit)

            submit(girder_token, cluster, job, log_url, config_url)
    except starcluster.exception.ClusterValidationError as ex:
        r = requests.patch(status_url, headers=headers,
                           json={'status': 'error'})
        # Log the error message
        log.error(ex.msg)
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)


@command.task
@cumulus.starcluster.logging.capture
def terminate_cluster(cluster, log_write_url=None, girder_token=None):
    name = cluster['name']
    cluster_id = cluster['_id']
    config_id = cluster['config']['_id']
    config_url = '%s/starcluster-configs/%s' \
        % (cumulus.config.girder.baseUrl, config_id)
    status_url = '%s/clusters/%s' \
        % (cumulus.config.girder.baseUrl, cluster_id)

    config_filepath = None
    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()

        headers = {'Girder-Token':  girder_token}
        r = requests.patch(status_url, headers=headers,
                           json={'status': 'terminating'})
        _check_status(r)

        start = time.time()

        with logstdout():
            cm.terminate_cluster(name, force=True)

        end = time.time()
        shutdown_time = end - start

        updates = {
            'timings': {
                'shutdown': int(round(shutdown_time * 1000))
            },
            'status': 'terminated'
        }

        # Now update the status of the cluster
        r = requests.patch(status_url, headers=headers, json=updates)
        # During terminate of a task the user may delete the cluster before its
        # terminated, so for now just ignore 404's when updated the status.
        if r.status_code != 404:
            _check_status(r)

    except starcluster.exception.ClusterDoesNotExist:
        r = requests.patch(status_url, headers=headers,
                           json={'status': 'terminated'})
        _check_status(r)
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)
