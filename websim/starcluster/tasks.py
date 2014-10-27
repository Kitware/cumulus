'''
Created on Oct 23, 2014

@author: cjh
'''

from cumulus.starcluster.logging import StarClusterLogHandler, StarClusterCallWriteHandler, logstdout, StarClusterLogFilter
import cumulus.starcluster.logging
from websim.celeryconfig import app
import starcluster.config
import starcluster.logger
import requests
import tempfile
import os
import logging
import threading
import uuid
import sys



@app.task
@cumulus.starcluster.logging.capture
def start_cluster(name, template, log_write_url=None, status_url=None):
    default_config_url = "http://0.0.0.0:8080/api/v1/file/544e41b6ff34c706dd4f79bc/download"

    config_filepath = None

    print >> sys.stderr, template

    try:

        r = requests.get(default_config_url)
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
        r = requests.put(status_url, data={'status': 'running'})
        r.raise_for_status()
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@app.task
@cumulus.starcluster.logging.capture
def terminate_cluster(name, log_write_url=None, status_url=None):
    default_config_url = "http://0.0.0.0:8080/api/v1/file/544e41b6ff34c706dd4f79bc/download"


    try:

        r = requests.get(default_config_url)
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
        r = requests.put(status_url, data={'status': 'terminated'})
        r.raise_for_status()
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)
