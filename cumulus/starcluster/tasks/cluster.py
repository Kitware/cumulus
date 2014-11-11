from cumulus.starcluster.logging import StarClusterLogHandler, StarClusterCallWriteHandler, logstdout, StarClusterLogFilter
import cumulus.starcluster.logging
from cumulus.starcluster.tasks.common import _write_config_file, _check_status, _log_exception
from cumulus.starcluster.tasks.job import submit
from cumulus.starcluster.tasks.celery import app
import cumulus.girderclient
import starcluster.config
import starcluster.logger
import starcluster.exception
import requests
import tempfile
import os
import logging
import threading
import uuid
import sys
import re
import inspect
import time
import traceback


@app.task
@cumulus.starcluster.logging.capture
def start_cluster(cluster, base_url=None, log_write_url=None, girder_token=None,
                  on_start_submit=None):
    config_filepath = None
    name = cluster['name']
    template = cluster['template']
    cluster_id = cluster['_id']
    config_id = cluster['configId']
    log_write_url = '%s/clusters/%s/log' % (base_url, cluster_id)
    config_url = '%s/starcluster-configs/%s' % (base_url, config_id)
    status_url = '%s/clusters/%s' % (base_url, cluster_id)

    try:

        config_filepath = _write_config_file(girder_token, config_url)
        headers = {'Girder-Token':  girder_token}
        r = requests.patch(status_url, headers=headers, json={'status': 'initializing'})
        _check_status(r)

        config = starcluster.config.StarClusterConfig(config_filepath)

        config.load()
        sc = config.get_cluster_template(template, name)

        result = sc.is_valid()

        with logstdout():
            sc.start()

        # Now update the status of the cluster
        r = requests.patch(status_url, headers=headers, json={'status': 'running'})
        _check_status(r)

        # Now if we have a job to submit do it!
        if on_start_submit:
            job_url = '%s/jobs/%s' % (base_url, on_start_submit)

            # Get the Job information
            r = requests.get(job_url, headers=headers)
            _check_status(r)
            job = r.json()
            log_url = '%s/jobs/%s/log' % (base_url, on_start_submit)

            submit(cluster, job, base_url, log_url, config_url, girder_token)
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@app.task
@cumulus.starcluster.logging.capture
def terminate_cluster(cluster, base_url=None, log_write_url=None, girder_token=None):
    name = cluster['name']
    cluster_id = cluster['_id']
    config_id = cluster['configId']
    log_write_url = '%s/clusters/%s/log' % (base_url, cluster_id)
    config_url = '%s/starcluster-configs/%s' % (base_url, config_id)
    status_url = '%s/clusters/%s' % (base_url, cluster_id)


    config_filepath = None
    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()


        with logstdout():
            cm.terminate_cluster(name, force=True)

        # Now update the status of the cluster
        headers = {'Girder-Token':  girder_token}
        r = requests.patch(status_url, headers=headers, json={'status': 'terminated'})
        _check_status(r)
    except starcluster.exception.ClusterDoesNotExist:
        r = requests.patch(status_url, headers=headers, json={'status': 'terminated'})
        _check_status(r)
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)


