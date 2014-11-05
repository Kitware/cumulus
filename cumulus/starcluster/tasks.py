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

        # Now if we have job to submit do it!
        if on_start_submit:
            job_url = '%s/jobs/%s' % (base_url, on_start_submit)

            # Get the Job information
            r = requests.get(job_url, headers=headers)
            _check_status(r)
            job = r.json()
            log_url = '%s/jobs/%s/log' % (base_url, on_start_submit)
            submit_job.delay(cluster, job, log_write_url=log_url,
                       config_url=config_url, girder_token=girder_token,
                       base_url=base_url)

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

@app.task
@cumulus.starcluster.logging.capture
def submit_job(cluster, job, log_write_url=None, config_url=None,
               girder_token=None, base_url=None):
    log = starcluster.logger.get_starcluster_logger()
    config_filepath = None
    script_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    status_url = '%s/jobs/%s' % (base_url, job_id)
    name = cluster['name']

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)

        # Write out script to upload to master

        (fd, script_filepath)  = tempfile.mkstemp()
        script_name = job_name = job['name']
        script_filepath = os.path.join(tempfile.gettempdir(), script_name)

        with open(script_filepath, 'w') as fp:
            for command in job['commands']:
                fp.write('%s\n' % command)

        # TODO should we log this to the cluster resource or a separte job log?
        with logstdout():
            config.load()
            cm = config.get_cluster_manager()
            sc = cm.get_cluster(name)
            master = sc.master_node

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

            log.info('Submitting "%s" to run on %s nodes' % (script_name, slots))

            output = master.ssh.execute(cmd)

            if len(output) != 1:
                raise Exception('Unexpected output: %s' % output)

            m = re.match('^[Yy]our job (\\d+)', output[0])
            if not m:
                raise Exception('Unable to extraction job id from: %s' % output[0])

            sge_id = m.group(1)

            # Update the state and sge id

            r = requests.patch(status_url, headers=headers, json={'status': 'queued', 'sgeId': sge_id})
            _check_status(r)
            job = r.json()

            # Now monitor the jobs progress
            monitor_job.s(cluster, job, config_url=config_url,
                          girder_token=girder_token, log_write_url=log_write_url,
                          job_dir=job_dir, base_url=base_url).apply_async(countdown=5)

        # Now update the status of the cluster
        headers = {'Girder-Token':  girder_token}
        r = requests.patch(status_url, headers=headers, json={'status': 'queued'})
        _check_status(r)
    except starcluster.exception.RemoteCommandFailed as ex:
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
    except starcluster.exception.ClusterDoesNotExist as ex:
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
    except Exception as ex:
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
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
def monitor_job(task, cluster, job, log_write_url=None, config_url=None, girder_token=None,
                job_dir=None, base_url=None):
    log = starcluster.logger.get_starcluster_logger()
    config_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    sge_id = job['sgeId']
    job_name = job['name']
    status_url = '%s/jobs/%s' % (base_url, job_id)
    name = cluster['name']

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()
        sc = cm.get_cluster(name)
        master = sc.master_node

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
            log.info('Jobs "%s" complete' % job_name)
            upload_job_output.delay(cluster, job, base_url=base_url,
                                    log_write_url=log_write_url,
                                    config_url=config_url,
                                    girder_token=girder_token,
                                    job_dir=job_dir)

        r = requests.patch(status_url, headers=headers, json={'status': status})
        _check_status(r)
    except starcluster.exception.RemoteCommandFailed as ex:
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
    except Exception as ex:
        print  >> sys.stderr,  ex
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
        raise
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@app.task
@cumulus.starcluster.logging.capture
def upload_job_output(cluster, job, base_url=None, log_write_url=None, config_url=None, girder_token=None, job_dir=None):
    log = starcluster.logger.get_starcluster_logger()
    config_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    status_url = '%s/jobs/%s' % (base_url, job_id)
    job_name = job['name']
    name = cluster['name']

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()
        sc = cm.get_cluster(name)
        master = sc.master_node

        # First put girder client on master
        path = inspect.getsourcefile(cumulus.girderclient)
        master.ssh.put(path)

        log.info('Uploading output for "%s"' % job_name)

        upload_cmd = 'python girderclient.py --token %s --url "%s" upload --dir %s  --item %s' \
                        % (girder_token, base_url, job_dir, job['output']['itemId'])

        upload_output = '%s.upload.out' % job_id
        upload_cmd = 'nohup %s  &> %s  &\n' % (upload_cmd, upload_output)

        with tempfile.NamedTemporaryFile() as upload_script:
            script_name = os.path.basename(upload_script.name)
            upload_script.write(upload_cmd)
            upload_script.write('echo $!\n')
            upload_script.flush()
            master.ssh.put(upload_script.name)

        upload_cmd = './%s' % script_name
        master.ssh.chmod(700, upload_cmd)
        output = master.ssh.execute(upload_cmd)

        if len(output) != 1:
            raise Exception('PID not returned by execute command')

        try:
            pid = int(output[0])
        except ValueError:
            raise Exception('Unable to extract PID from: %s' % output)

        on_complete = None
        if 'onComplete' in job and 'cluster' in job['onComplete'] and \
            job['onComplete']['cluster'] == 'terminate':
            on_complete = terminate_cluster.s(cluster, base_url=base_url,
                                              log_write_url=log_write_url,
                                              girder_token=girder_token)

        monitor_process(name, job, pid, upload_output, log_write_url=log_write_url,
                        config_url=config_url, girder_token=girder_token,
                        base_url=base_url, on_complete=on_complete)

    except starcluster.exception.RemoteCommandFailed as ex:
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@app.task(bind=True, max_retries=None)
@cumulus.starcluster.logging.capture
def monitor_process(task, name, job, pid, nohup_out, log_write_url=None, config_url=None,
                    girder_token=None, base_url=None, on_complete=None):
    log = starcluster.logger.get_starcluster_logger()
    config_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    sge_id = job['sgeId']
    job_name = job['name']
    status_url = '%s/jobs/%s' % (base_url, job_id)

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()
        sc = cm.get_cluster(name)
        master = sc.master_node

        # See if the process is still running
        output = master.ssh.execute('ps %s | grep %s' % (pid, pid),
                                    ignore_exit_status=True, source_profile=False)

        if len(output) > 0:
            # Process is still running so schedule self again in about 5 secs
            # N.B. throw=False to prevent Retry exception being raised
            task.retry(throw=False, countdown=5)
        else:
            try:
                master.ssh.get(nohup_out)
                # Log the output
                with open(nohup_out, 'r') as fp:
                    log.info('Job process output: %s' % fp.read())
            finally:
                if nohup_out and os.path.exists(nohup_out):
                    os.remove(nohup_out)

            # Fire off the on_compete task if we have one
            if on_complete:
                on_complete.delay()
    except starcluster.exception.RemoteCommandFailed as ex:
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
    except Exception as ex:
        print  >> sys.stderr,  ex
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
        raise
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

