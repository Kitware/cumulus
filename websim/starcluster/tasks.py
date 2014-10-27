'''
Created on Oct 23, 2014

@author: cjh
'''

from cumulus.starcluster.logging import StarClusterLogHandler, StarClusterCallWriteHandler, logstdout
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
def start_cluster(cluster_template, cluster_name):
    default_config_url = "http://0.0.0.0:8080/api/v1/file/544e41b6ff34c706dd4f79bc/download"
    api_url = 'http://0.0.0.0:8080/api/v1'
    cluster_id = '544e66baff34c74de4497a47'

    log_write_url = '%s/clusters/%s/log' % (api_url, cluster_id)

    logger = starcluster.logger.get_starcluster_logger()
    logger.setLevel(logging.INFO)
    logger.addHandler(StarClusterLogHandler(log_write_url, logging.DEBUG))

    sys.stdout = StarClusterCallWriteHandler()
    call_id = uuid.uuid4()
    threadlocal = threading.local()
    threadlocal.id = call_id

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
        if os.path.exists(config_filepath):
            os.remove(config_filepath)

@app.task
def terminate_cluster():
    pass
