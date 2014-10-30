'''
Created on Oct 23, 2014

@author: cjh
'''

from cumulus.starcluster.logging import StarClusterLogHandler, StarClusterCallWriteHandler, logstdout, StarClusterLogFilter
import cumulus.starcluster.logging
from cumulus.celeryconfig import app
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
        r = requests.put(status_url, headers=headers, params={'status': 'initializing'})
        r.raise_for_status()

        config = starcluster.config.StarClusterConfig(config_filepath)

        config.load()
        sc = config.get_cluster_template(template, name)

        result = sc.is_valid()

        with logstdout():
            sc.start()

        # Now update the status of the cluster
        r = requests.put(status_url, headers=headers, params={'status': 'running'})
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
        r = requests.put(status_url, headers=headers, params={'status': 'terminated'})
        r.raise_for_status()
    except starcluster.exception.ClusterDoesNotExist:
        r = requests.put(status_url, headers=headers, params={'status': 'terminated'})
        r.raise_for_status()
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@app.task
@cumulus.starcluster.logging.capture
def submit_job(name, job, log_write_url=None, config_url=None,
               girder_token=None, base_url=None):

    config_filepath = None
    script_filepath = None
    headers = {'Girder-Token':  girder_token}
    status_url = '%s/jobs/%s' % (base_url, job['_id'])
    job_id = job['_id']

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)

        # Write out script to upload to master

        (fd, script_filepath)  = tempfile.mkstemp()
        script_name = job_name = job['name']
        script_filepath = os.path.join(tempfile.gettempdir(), script_name)

        with open(script_filepath, 'w') as fp:
            fp.write(job['commands'])

        # TODO should we log this to the cluster resource or a separte job log?
        with logstdout():
            config.load()
            cm = config.get_cluster_manager()
            cluster = cm.get_cluster(name)
            master = cluster.master_node

            # Create job directory
            job_dir = os.path.join(job['_id'], time.strftime('%Y-%m-%d-%H-%M-%S'))
            master.ssh.makedirs(job_dir)

            # put the script to master
            master.ssh.put(script_filepath, job_dir)
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

            stdout_file = '%s/%s.o' % (job_dir, script_name)
            stderr_file = '%s/%s.e' % (job_dir, script_name)

            cmd = 'cd %s && qsub -o %s, -e %s -pe orte %s ./%s' \
                        % (job_dir, stdout_file, stderr_file, slots, script_name)

            print >> sys.stderr,  cmd

            output = master.ssh.execute(cmd)

            if len(output) != 1:
                raise Exception('Unexpected output: %s' % output)

            m = re.match('^[Yy]our job (\\d+)', output[0])
            if not m:
                raise Exception('Unable to extraction job id from: %s' % output[0])

            sge_id = m.group(1)

            # Update the state and sge id

            r = requests.patch(status_url, headers=headers, json={'status': 'queued', 'sgeId': sge_id})
            r.raise_for_status()
            job = r.json()

            # Now monitor the jobs progress
            monitor_job.s(name, job, config_url=config_url,
                              girder_token=girder_token, log_write_url=log_write_url,
                              job_dir=job_dir, base_url=base_url).apply_async(countdown=5)

        # Now update the status of the cluster
        headers = {'Girder-Token':  girder_token}
        r = requests.patch(status_url, headers=headers, json={'status': 'queued'})
        r.raise_for_status()
    except starcluster.exception.RemoteCommandFailed as ex:
        print  >> sys.stderr,  ex.message
    except starcluster.exception.ClusterDoesNotExist as ex:
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        print  >> sys.stderr,  ex.message
        r.raise_for_status()
    except Exception as ex:
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        print  >> sys.stderr,  ex.message
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
def monitor_job(task, name, job, log_write_url=None, config_url=None, girder_token=None,
                job_dir=None, base_url=None):
    config_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    sge_id = job['sgeId']
    status_url = '%s/jobs/%s' % (base_url, job_id)

    print job

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
            if m and m.group(1) == sge_id:
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
        else:
            # Fire off task to upload the output
            upload_job_output.delay(name, job, base_url=base_url,
                                    log_write_url=log_write_url,
                                    config_url=config_url,
                                    girder_token=girder_token,
                                    job_dir=job_dir)

        r = requests.patch(status_url, headers=headers, json={'status': status})
        r.raise_for_status()
    except starcluster.exception.RemoteCommandFailed as ex:
        print  >> sys.stderr,  ex.message
    except Exception as ex:
        print  >> sys.stderr,  ex
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        r.raise_for_status()
        raise
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@app.task
@cumulus.starcluster.logging.capture
def upload_job_output(name, job, base_url=None, log_write_url=None, config_url=None, girder_token=None, job_dir=None):
    config_filepath = None
    headers = {'Girder-Token':  girder_token}
    collection_id = job['outputCollectionId']
    job_id = job['_id']

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()
        cluster = cm.get_cluster(name)
        master = cluster.master_node

        # First put girder client on master
        path = inspect.getsourcefile(cumulus.girderclient)
        master.ssh.put(path)

        upload_cmd = 'python girderclient.py --dir %s --url "%s" --collection %s --token %s --folder %s' \
                        % (job_dir, base_url, collection_id, girder_token, job_id)
        print  >> sys.stderr, "cmd: %s" % upload_cmd
        print  >> sys.stderr, master.ssh.execute(upload_cmd)
    except starcluster.exception.RemoteCommandFailed as ex:
        print  >> sys.stderr,  ex.message
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)
