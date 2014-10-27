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
def start_cluster(cluster_template, cluster_name, log_write_url=None):
    default_config_url = "http://0.0.0.0:8080/api/v1/file/544e41b6ff34c706dd4f79bc/download"

    config_filepath = None

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
        sc = config.get_cluster_template(cluster_template, cluster_template)

        result = sc.is_valid()

        print result

        #sc.start(create=cluster_name)
        with logstdout():
            config.get_cluster_manager().list_clusters()
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@app.task
def terminate_cluster():
    pass
