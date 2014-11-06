from cumulus.starcluster.logging import StarClusterLogHandler, StarClusterCallWriteHandler, logstdout, StarClusterLogFilter
import cumulus.starcluster.logging
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

def _write_config_file(girder_token, config_url):
        headers = {'Girder-Token':  girder_token}

        r = requests.get(config_url, headers=headers, params={'format': 'ini'})
        r.raise_for_status()

        # Write config to temp file
        (fd, config_filepath)  = tempfile.mkstemp()

        try:
            os.write(fd, r.text)
        finally:
            os.close(fd)

        return config_filepath

def _log_exception(ex):
    log = starcluster.logger.get_starcluster_logger()
    log.error(traceback.format_exc())

def _check_status(request):
    if request.status_code != 200:
        print sys.stderr, request.json()
        request.raise_for_status()

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

