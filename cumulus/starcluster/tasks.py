'''
Created on Oct 23, 2014

@author: cjh
'''

from cumulus.starcluster.logging import StarClusterLogHandler, StarClusterCallWriteHandler, logstdout, StarClusterLogFilter
import cumulus.starcluster.logging
from cumulus.celeryconfig import app
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
from StringIO import StringIO


def _write_config_file(girder_token, config_url):
        headers = {'Girder-Token':  girder_token}

        r = requests.get(config_url, headers=headers)
        r.raise_for_status()

        # Write config to temp file
        (fd, config_filepath)  = tempfile.mkstemp()

        try:
            os.write(fd, r.text)
        finally:
            os.close(fd)

        return config_filepath


@app.task
@cumulus.starcluster.logging.capture
def start_cluster(name, template, log_write_url=None, status_url=None, config_url=None,
                  girder_token=None):
    config_filepath = None

    try:

        config_filepath = _write_config_file(girder_token, config_url)
        headers = {'Girder-Token':  girder_token}
        r = requests.put(status_url, headers=headers, data={'status': 'initializing'})
        r.raise_for_status()

        config = starcluster.config.StarClusterConfig(config_filepath)

        config.load()
        sc = config.get_cluster_template(template, name)

        result = sc.is_valid()

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
        r = requests.put(status_url, headers=headers, data={'status': 'terminated'})
        r.raise_for_status()
    except starcluster.exception.ClusterDoesNotExist:
        r = requests.put(status_url, headers=headers, data={'status': 'terminated'})
        r.raise_for_status()
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@app.task
@cumulus.starcluster.logging.capture
def submit_job(name, script, log_write_url=None, status_url=None, config_url=None,
               girder_token=None, jobid_url=None):

    config_filepath = None
    script_filepath = None
    headers = {'Girder-Token':  girder_token}

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)

        # Write out script to upload to master
        (fd, script_filepath)  = tempfile.mkstemp()
        script_name = os.path.basename(script_filepath)

        try:
            os.write(fd, script)
        finally:
            os.close(fd)

        # TODO should we log this to the cluster resource or a separte job log?
        with logstdout():
            config.load()
            cm = config.get_cluster_manager()
            cluster = cm.get_cluster(name)
            master = cluster.master_node

            # put the script to master
            master.ssh.put(script_filepath)
            # Now submit the job
            output = master.ssh.execute('qconf -sp orte')
            slots = -1
            for line in output:
                m = re.match('slots[\s]+(\d)', line)
                if m:
                    slots = m.group(1)
                    break

            if slots < 0:
                raise Exception('Unable to retrieve number of slots')

            output = master.ssh.execute('qsub -pe orte %s ./%s' % (slots, script_name))

            if len(output) != 1:
                raise Exception('Unexpected output: %s' % output)

            m = re.match('^[Yy]our job (\\d+)', output[0])
            if not m:
                raise Exception('Unable to extraction job id from: %s' % output[0])

            job_id = m.group(1)

            r = requests.put(jobid_url, headers=headers, data={'sgeJobId': job_id})
            r.raise_for_status()

            # Update the state
            r = requests.put(status_url, headers=headers, data={'status': 'queued'})
            r.raise_for_status()

            # Now monitor the jobs progress
            monitor_job.delay(name, job_id, status_url=status_url, config_url=config_url,
                              girder_token=girder_token, log_write_url=log_write_url)

        # Now update the status of the cluster
        headers = {'Girder-Token':  girder_token}
        r = requests.put(status_url, headers=headers, data={'status': 'queued'})
        r.raise_for_status()
    except starcluster.exception.RemoteCommandFailed as ex:
        print  >> sys.stderr,  ex.message
    except starcluster.exception.ClusterDoesNotExist:
        r = requests.put(status_url, headers=headers, data={'status': 'error'})
        r.raise_for_status()
    except Exception as ex:
        r = requests.put(status_url, headers=headers, data={'status': 'error'})
        r.raise_for_status()
        raise
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)
        if script_filepath and os.path.exists(script_filepath):
            os.remove(script_filepath)

# Running states
running_state = ['r', 'd', 'e']

# Queued states
queued_state = ['qw', 'q', 'w', 's', 'h', 't']

@app.task(bind=True, max_retries=None)
@cumulus.starcluster.logging.capture
def monitor_job(task, name, job_id, status_url=None, log_write_url=None, config_url=None, girder_token=None):
    config_filepath = None
    headers = {'Girder-Token':  girder_token}

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()
        cluster = cm.get_cluster(name)
        master = cluster.master_node

        # TODO Work out how to pass a job id to qstat
        output = master.ssh.execute('qstat')

        state = None

        # Extract the state from the output
        for line in output:
            m = re.match('^\\s*(\\d+)\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\w+)', line)
            if m and m.group(1) == job_id:
                state = m.group(2)

        # If not in queue that status is complete
        status = 'complete'

        if state:
            if state in running_state:
                status = 'running'
            elif state in queued_state:
                status = 'queued'
            else:
                raise Exception('Unrecognized SGE state')

            # Job is still active so schedule self again in about 5 secs
            # N.B. throw=False to prevent Retry exception being raised
            task.retry(throw=False, countdown=5)

        r = requests.put(status_url, headers=headers, data={'status': status})
        r.raise_for_status()
    except starcluster.exception.RemoteCommandFailed as ex:
        print  >> sys.stderr,  ex.message
    except Exception as ex:
        print  >> sys.stderr,  ex
        r = requests.put(status_url, headers=headers, data={'status': 'error'})
        r.raise_for_status()
        raise
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)
