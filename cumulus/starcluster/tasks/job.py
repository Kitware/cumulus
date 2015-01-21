from __future__ import absolute_import
from cumulus.starcluster.logging import StarClusterLogHandler, StarClusterCallWriteHandler, logstdout, StarClusterLogFilter
import cumulus.starcluster.logging
from cumulus.starcluster.tasks.common import _write_config_file, _check_status, _log_exception
from cumulus.starcluster.tasks.celery import command, monitor
import cumulus
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
from celery import signature
import StringIO
from jinja2 import Template

def _put_script(ssh, script_commands):
    with tempfile.NamedTemporaryFile() as script:
        script_name = os.path.basename(script.name)
        script.write(script_commands)
        script.write('echo $!\n')
        script.flush()
        ssh.put(script.name)

        cmd = './%s' % script_name
        ssh.execute('chmod 700 %s' % cmd)

    return cmd

@command.task
@cumulus.starcluster.logging.capture
def download_job_input(cluster, job, log_write_url=None, config_url=None, girder_token=None):
    log = starcluster.logger.get_starcluster_logger()
    config_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)
    job_name = job['name']
    name = cluster['name']

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()
        sc = cm.get_cluster(name)
        master = sc.master_node
        master.user = sc.cluster_user

        # First put girder client on master
        path = inspect.getsourcefile(cumulus.girderclient)
        master.ssh.put(path)

        # Create job directory
        master.ssh.mkdir('./%s' % job_id)

        log.info('Downloading input for "%s"' % job_name)

        r = requests.patch(status_url, json={'status': 'downloading'}, headers=headers)
        _check_status(r)

        download_cmd = 'python girderclient.py --token %s --url "%s" download --dir %s  --job %s' \
                        % (girder_token, cumulus.config.girder.baseUrl, job_id, job_id)

        download_output = '%s.download.out' % job_id
        download_cmd = 'nohup %s  &> %s  &\n' % (download_cmd, download_output)

        download_cmd = _put_script(master.ssh, download_cmd)
        output = master.ssh.execute(download_cmd)

        # Remove download script
        master.ssh.unlink(download_cmd)

        if len(output) != 1:
            raise Exception('PID not returned by execute command')

        try:
            pid = int(output[0])
        except ValueError:
            raise Exception('Unable to extract PID from: %s' % output)

        # When the download is complete submit the job
        on_complete = submit_job.s(cluster, job, log_write_url=log_write_url,
                                   config_url=config_url, girder_token=girder_token)

        monitor_process.delay(name, job, pid, download_output, log_write_url=log_write_url,
                        config_url=config_url, on_complete=on_complete, girder_token=girder_token)

    except starcluster.exception.RemoteCommandFailed as ex:
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@command.task
@cumulus.starcluster.logging.capture
def submit_job(cluster, job, log_write_url=None, config_url=None, girder_token=None):
    log = starcluster.logger.get_starcluster_logger()
    config_filepath = None
    script_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    job_dir = job_id
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)
    name = cluster['name']

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)

        # Write out script to upload to master

        (fd, script_filepath)  = tempfile.mkstemp()
        script_name = job_name = job['name']
        script_filepath = os.path.join(tempfile.gettempdir(), script_name)

        script_template = StringIO.StringIO();
        for command in job['commands']:
            script_template.write('%s\n' % command)

        with logstdout():
            config.load()
            cm = config.get_cluster_manager()
            sc = cm.get_cluster(name)
            master = sc.master_node
            master.user = sc.cluster_user

            # First get number of slots available
            output = master.ssh.execute('qconf -sp orte')
            slots = -1
            for line in output:
                m = re.match('slots[\s]+(\d+)', line)
                if m:
                    slots = m.group(1)
                    break

            if slots < 1:
                raise Exception('Unable to retrieve number of slots')

            job_params = {}
            if 'params' in job:
                job_params = job['params']

            # Now we can template submission script
            script = Template(script_template.getvalue()).render(cluster=cluster,
                     job=job, base_url=cumulus.config.girder.baseUrl, number_of_slots=int(slots), **job_params)

            with open(script_filepath, 'w') as fp:
                fp.write('%s\n' % script)

            master.ssh.mkdir(job_id, ignore_failure=True)
            # put the script to master
            master.ssh.put(script_filepath, job_id)
            # Now submit the job

            log.info('We have %s slots available' % slots)

            cmd = 'cd %s && qsub -cwd ./%s' \
                        % (job_dir, script_name)

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

            job['queuedTime'] = time.time()

            # Now monitor the jobs progress
            monitor_job.s(cluster, job, config_url=config_url,
                          log_write_url=log_write_url,
                          girder_token=girder_token).apply_async(countdown=5)

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

def submit(girder_token, cluster, job, log_url, config_url):

    # Do we inputs to download ?
    if 'input' in job and len(job['input']) > 0:

        download_job_input.delay(cluster, job, log_write_url=log_url,
                                 config_url=config_url, girder_token=girder_token)
    else:
        submit_job.delay(cluster, job, log_write_url=log_url,
                         config_url=config_url, girder_token=girder_token)

# Running states
running_state = ['r', 'd', 'e']

# Queued states
queued_state = ['qw', 'q', 'w', 's', 'h', 't']

@monitor.task(bind=True, max_retries=None)
@cumulus.starcluster.logging.capture
def monitor_job(task, cluster, job, log_write_url=None, config_url=None, girder_token=None):
    log = starcluster.logger.get_starcluster_logger()
    config_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    job_dir = job_id
    sge_id = job['sgeId']
    job_name = job['name']
    status_update_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)
    status_url = '%s/jobs/%s/status' % (cumulus.config.girder.baseUrl, job_id)
    name = cluster['name']

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()
        sc = cm.get_cluster(name)
        master = sc.master_node
        master.user = sc.cluster_user

        # TODO Work out how to pass a job id to qstat
        output = master.ssh.execute('qstat')

        state = None

        # Extract the state from the output
        for line in output:
            m = re.match('^\\s*(\\d+)\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\w+)', line)
            if m and m.group(1) == sge_id:
                state = m.group(2)

        # First get the current status
        r = requests.get(status_url, headers=headers)
        _check_status(r)

        current_status = r.json()['status']

        # If not in queue and we are terminating then move to terminated
        if current_status == 'terminating':
            status = 'terminated'
        # Otherwise we are complete
        else:
            status = 'complete'

        timings = {}

        if state and current_status != 'terminating':
            if state in running_state:
                status = 'running'
                if 'queuedTime' in job:
                    queued_time = time.time() - job['queuedTime']
                    timings = {
                        'queued': int(round(queued_time * 1000))
                    }
                    del job['queuedTime']
                    job['runningTime'] = time.time()
            elif state in queued_state:
                status = 'queued'
            else:
                raise Exception('Unrecognized SGE state')

            # Job is still active so schedule self again in about 5 secs
            # N.B. throw=False to prevent Retry exception being raised
            task.retry(throw=False, countdown=5)
        else:
            if status == 'complete':
                if 'runningTime' in job:
                    running_time = time.time() - job['runningTime']
                    timings = {
                        'running': int(round(running_time * 1000))
                    }
                    del job['runningTime']
                # Fire off task to upload the output
                log.info('Jobs "%s" complete' % job_name)
                upload_job_output.delay(cluster, job, log_write_url=log_write_url,
                                        config_url=config_url,
                                        job_dir=job_dir, girder_token=girder_token)

        # Do we need to tail any output files
        for output in job['output']:
            if 'tail' in output and output['tail']:
                path = output['path']
                offset = 0
                if 'content' in output:
                    offset = len(output['content'])
                else:
                    output['content'] = []
                tail_path = '%s/%s' % (job_id, path)
                command = 'tail -n +%d %s' % (offset, tail_path)
                try:
                    # Only tail if file exists
                    if master.ssh.isfile(tail_path):
                        stdout = master.ssh.execute(command)
                        output['content'] = output['content'] + stdout
                    else:
                        log.info('Skipping tail of %s as file doesn\'t currently exist' % tail_path)
                except starcluster.exception.RemoteCommandFailed as ex:
                    _log_exception(ex)

        json = {
            'status': status,
            'output': job['output'],
            'timings': timings
        }

        r = requests.patch(status_update_url, headers=headers, json=json)
        _check_status(r)
    except starcluster.exception.RemoteCommandFailed as ex:
        r = requests.patch(status_update_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
    except Exception as ex:
        print  >> sys.stderr,  ex
        r = requests.patch(status_update_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
        raise
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@command.task
@cumulus.starcluster.logging.capture
def upload_job_output(cluster, job, log_write_url=None, config_url=None, job_dir=None, girder_token=None):
    log = starcluster.logger.get_starcluster_logger()
    config_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)
    job_name = job['name']
    name = cluster['name']

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()
        sc = cm.get_cluster(name)
        master = sc.master_node
        master.user = sc.cluster_user

        # First put girder client on master
        path = inspect.getsourcefile(cumulus.girderclient)
        master.ssh.put(path)

        log.info('Uploading output for "%s"' % job_name)

        cmds = ['cd %s' % job_dir]
        upload_cmd = 'python ../girderclient.py --token %s --url "%s" upload --job %s' \
                        % (girder_token, cumulus.config.girder.baseUrl, job['_id'])

        upload_output = '%s.upload.out' % job_id
        cmds.append('nohup %s  &> ../%s  &\n' % (upload_cmd, upload_output))

        upload_cmd = _put_script(master.ssh, '\n'.join(cmds))
        output = master.ssh.execute(upload_cmd)

        # Remove upload script
        master.ssh.unlink(upload_cmd)

        if len(output) != 1:
            raise Exception('PID not returned by execute command')

        try:
            pid = int(output[0])
        except ValueError:
            raise Exception('Unable to extract PID from: %s' % output)

        on_complete = None
        if 'onComplete' in job and 'cluster' in job['onComplete'] and \
            job['onComplete']['cluster'] == 'terminate':
            cluster_log_url = '%s/clusters/%s/log' % (cumulus.config.girder.baseUrl, cluster['_id'])
            on_complete = signature(
                'cumulus.starcluster.tasks.cluster.terminate_cluster',
                args=(cluster,), kwargs={'log_write_url': cluster_log_url,
                                         'girder_token': girder_token})

        monitor_process.delay(name, job, pid, upload_output, log_write_url=log_write_url,
                        config_url=config_url, on_complete=on_complete, girder_token=girder_token)

    except starcluster.exception.RemoteCommandFailed as ex:
        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
        _check_status(r)
        _log_exception(ex)
    finally:
        if config_filepath and os.path.exists(config_filepath):
            os.remove(config_filepath)

@monitor.task(bind=True, max_retries=None)
@cumulus.starcluster.logging.capture
def monitor_process(task, name, job, pid, nohup_out, log_write_url=None, config_url=None,
                    on_complete=None, output_message='Job download/upload error: %s',
                    girder_token=None):
    log = starcluster.logger.get_starcluster_logger()
    config_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    job_name = job['name']
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)
        config.load()
        cm = config.get_cluster_manager()
        sc = cm.get_cluster(name)
        master = sc.master_node
        master.user = sc.cluster_user

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
                    output = fp.read()
                    if output.strip():
                        log.error(output_message % output)
                        # If we have output then set the error state on the
                        # job and return
                        r = requests.patch(status_url, headers=headers, json={'status': 'error'})
                        _check_status(r)
                        return
            finally:
                if nohup_out and os.path.exists(nohup_out):
                    os.remove(nohup_out)

            # Fire off the on_compete task if we have one
            if on_complete:
                signature(on_complete).delay()
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

@command.task
@cumulus.starcluster.logging.capture
def terminate_job(cluster, job, log_write_url=None, config_url=None, girder_token=None):
    log = starcluster.logger.get_starcluster_logger()
    config_filepath = None
    script_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    job_dir = job_id
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)
    name = cluster['name']

    print >> sys.stderr, name

    try:
        config_filepath = _write_config_file(girder_token, config_url)
        config = starcluster.config.StarClusterConfig(config_filepath)

        with logstdout():
            config.load()
            cm = config.get_cluster_manager()
            sc = cm.get_cluster(name)
            master = sc.master_node
            master.user = sc.cluster_user

            if 'sgeId' not in job:
                raise Exception('Job doesn\'t have a sge id')

            # First get number of slots available
            output = master.ssh.execute('qdel %s' % job['sgeId'])

            if 'onTerminate' in job:
                commands = '\n'.join(job['onTerminate']['commands']) + '\n'
                commands = Template(commands).render(cluster=cluster,
                     job=job, base_url=cumulus.config.girder.baseUrl)

                on_terminate = _put_script(master.ssh, commands+'\n')

                terminate_output = '%s.terminate.out' % job_id
                terminate_cmd = 'nohup %s  &> %s  &\n' % (on_terminate, terminate_output)
                terminate_cmd = _put_script(master.ssh, terminate_cmd)
                output = master.ssh.execute(terminate_cmd)

                master.ssh.unlink(on_terminate)
                master.ssh.unlink(terminate_cmd)

                if len(output) != 1:
                    raise Exception('PID not returned by execute command')

                try:
                    pid = int(output[0])
                except ValueError:
                    raise Exception('Unable to extract PID from: %s' % output)

                monitor_process.delay(name, job, pid, terminate_output, log_write_url=log_write_url,
                                config_url=config_url, output_message='onTerminate error: %s',
                                girder_token=girder_token)

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


