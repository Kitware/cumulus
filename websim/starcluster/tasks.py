'''
Created on Oct 23, 2014

@author: cjh
'''

from cumulus.starcluster.logging import StarClusterLogHandler, StarClusterCallWriteHandler, logstdout, StarClusterLogFilter
import cumulus.starcluster.logging
from websim.celeryconfig import app
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



@app.task
@cumulus.starcluster.logging.capture
def start_cluster(name, template, log_write_url=None, status_url=None, config_url=None,
                  girder_token=None):
    config_filepath = None

    try:
        headers = {'Girder-Token':  girder_token}
        r = requests.get(config_url, headers=headers)
        r.raise_for_status()

        r = requests.put(status_url, headers=headers, data={'status': 'initializing'})
        r.raise_for_status()

        # Write config to temp file
        (fd, config_filepath)  = tempfile.mkstemp()

        try:
            os.write(fd, r.text)
        finally:
            os.close(fd)

        config = starcluster.config.StarClusterConfig(config_filepath)

        config.load()
        sc = config.get_cluster_template(template, name)

        result = sc.is_valid()

        print result

        with logstdout():
            sc.start()

        # Now update the status of the cluster
        r = requests.put(status_url, headers=headers, data={'status': 'running'})
        r.raise_for_status()
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@app.task
@cumulus.starcluster.logging.capture
def terminate_cluster(name, log_write_url=None, status_url=None, config_url=None,
                      girder_token=None):

    try:
        headers = {'Girder-Token':  girder_token}
        r = requests.get(config_url, headers=headers)
        r.raise_for_status()

        # Write config to temp file
        (fd, config_filepath)  = tempfile.mkstemp()


        try:
            os.write(fd, r.text)
        finally:
            os.close(fd)

        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()


        with logstdout():
            terminate_cluster
            cm.terminate_cluster(name, force=True)

        # Now update the status of the cluster
        r = requests.put(status_url, headers=headers, data={'status': 'terminated'})
        r.raise_for_status()
    except starcluster.exception.ClusterDoesNotExist:
        r = requests.put(status_url, headers=headers, data={'status': 'terminated'})
        r.raise_for_status()
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)
