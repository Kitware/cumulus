#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2015 Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the "License" );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################

from __future__ import absolute_import
import traceback
from cumulus.common import check_status
from cumulus.common import get_post_logger, get_job_logger
from cumulus.common import get_cluster_logger
from cumulus.celery import command, monitor
import cumulus
import cumulus.girderclient
import cumulus.constants
from cumulus.constants import ClusterType, JobQueueState
from cumulus.queue import get_queue_adapter
from cumulus.queue.abstract import AbstractQueueAdapter
from cumulus.transport import get_connection
from cumulus.transport.files.download import download_path
from cumulus.transport.files.upload import upload_path
from cumulus.transport.files import get_assetstore_url_base, get_assetstore_id
import requests
import os
import re
import inspect
import time
import uuid
from six import StringIO
from celery import signature
from celery.exceptions import Retry
from jinja2 import Environment, Template, PackageLoader
from jsonpath_rw import parse
import tempfile
from girder_client import HttpError
import paramiko


def _put_script(conn, script_commands):
    script_name = uuid.uuid4().hex
    script = script_commands + 'echo $!\n'
    conn.put(StringIO(script), script_name)
    cmd = './%s' % script_name
    conn.execute('chmod 700 %s' % cmd)

    return cmd


def job_directory(cluster, job, user_home='.'):
    """
    Returns the job directory for a given job.

    :param cluster: The job the cluster is running on.
    :param job: The job to return the directory for.

    """
    # First try the job parameters
    output_root = parse('params.jobOutputDir').find(job)
    if output_root:
        output_root = output_root[0].value
    else:
        # Try the cluster
        output_root = parse('config.jobOutputDir').find(cluster)
        if output_root:
            output_root = output_root[0].value
        else:
            output_root = user_home

    return os.path.join(output_root, job['_id'])


def download_job_input_items(cluster, job, log_write_url=None,
                             girder_token=None):
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)

    try:
        with get_connection(girder_token, cluster) as conn:
            # First put girder client on master
            path = inspect.getsourcefile(cumulus.girderclient)
            with open(path, 'r') as fp:
                conn.put(fp, os.path.basename(path))

            r = requests.patch(status_url, json={'status': 'downloading'},
                               headers=headers)
            check_status(r)

            download_cmd = 'python girderclient.py --token %s --url "%s" ' \
                           'download --dir %s  --job %s' \
                % (girder_token, cumulus.config.girder.baseUrl,
                   job_directory(cluster, job), job_id)

            download_output = '%s.download.out' % job_id
            download_cmd = 'nohup %s  &> %s  &\n' % (download_cmd,
                                                     download_output)

            download_cmd = _put_script(conn, download_cmd)
            output = conn.execute(download_cmd)

            # Remove download script
            conn.remove(download_cmd)

        if len(output) != 1:
            raise Exception('PID not returned by execute command')

        try:
            pid = int(output[0])
        except ValueError:
            raise Exception('Unable to extract PID from: %s' % output)

        # When the download is complete submit the job
        on_complete = submit_job.s(cluster, job, log_write_url=log_write_url,
                                   girder_token=girder_token)

        monitor_process.delay(cluster, job, pid, download_output,
                              log_write_url=log_write_url,
                              on_complete=on_complete,
                              girder_token=girder_token)

    except Exception as ex:
        r = requests.patch(status_url, headers=headers,
                           json={'status': 'error'})
        check_status(r)
        get_job_logger(job, girder_token).exception(str(ex))


def download_job_input_folders(cluster, job, log_write_url=None,
                               girder_token=None, submit=True):
    job_dir = job_directory(cluster, job)

    with get_connection(girder_token, cluster) as conn:
        for input in job['input']:
            if 'folderId' in input and 'path' in input:
                folder_id = input['folderId']
                path = input['path']
                upload_path(conn, girder_token, folder_id,
                            os.path.join(job_dir, path))

    if submit:
        submit_job.delay(cluster, job, log_write_url=log_write_url,
                         girder_token=girder_token)


@command.task
def download_job_input(cluster, job, log_write_url=None, girder_token=None):
    job_url = '%s/jobs/%s/log' % (cumulus.config.girder.baseUrl, job['_id'])
    log = get_post_logger(job['_id'], girder_token, job_url)

    # Create job directory
    with get_connection(girder_token, cluster) as conn:
        conn.makedirs(job_directory(cluster, job))

    log.info('Downloading input for "%s"' % job['name'])

    if parse('input.itemId').find(job):
        download_job_input_items(cluster, job, log_write_url=log_write_url,
                                 girder_token=girder_token)
    else:
        download_job_input_folders(cluster, job, log_write_url=log_write_url,
                                   girder_token=girder_token)


def _get_parallel_env(cluster, job):
    parallel_env = None
    if 'parallelEnvironment' in job.get('params', {}):
        parallel_env = job['params']['parallelEnvironment']
    elif 'parallelEnvironment' in cluster['config']:
        parallel_env = cluster['config']['parallelEnvironment']

    # if this is a ec2 cluster then we can default to orte
    if not parallel_env and cluster['type'] == ClusterType.EC2:
        parallel_env = 'orte'

    return parallel_env


def _is_terminating(job, girder_token):
    headers = {'Girder-Token':  girder_token}
    status_url = '%s/jobs/%s/status' % (cumulus.config.girder.baseUrl,
                                        job['_id'])
    r = requests.get(status_url, headers=headers)
    check_status(r)
    current_status = r.json()['status']

    return current_status in [JobState.TERMINATED, JobState.TERMINATING]


def _generate_submission_script(job, cluster, job_params):
    env = Environment(loader=PackageLoader('cumulus', 'templates'))
    template = env.get_template('template.sh')
    script = template.render(cluster=cluster, job=job,
                             baseUrl=cumulus.config.girder.baseUrl,
                             **job_params)

    # We now render again to ensure any template variable in the jobs
    # commands are filled out.
    script = Template(script).render(cluster=cluster, job=job,
                                     baseUrl=cumulus.config.girder.baseUrl,
                                     **job_params)

    return script


def _get_on_complete(job):
    on_complete = parse('onComplete.cluster').find(job)

    if on_complete:
        on_complete = on_complete[0].value
    else:
        on_complete = None

    return on_complete


@command.task
def submit_job(cluster, job, log_write_url=None, girder_token=None,
               monitor=True):
    job_url = '%s/jobs/%s/log' % (cumulus.config.girder.baseUrl, job['_id'])
    log = get_post_logger(job['_id'], girder_token, job_url)
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)
    try:
        # if terminating break out
        if _is_terminating(job, girder_token):
            return

        script_name = job['name']

        with get_connection(girder_token, cluster) as conn:
            job_params = {}
            if 'params' in job:
                job_params = job['params']

            output = conn.execute('pwd')
            if len(output) != 1:
                raise Exception('Unable to fetch users home directory.')

            user_home = output[0].strip()
            job_dir = job_directory(cluster, job, user_home=user_home)
            job['dir'] = job_dir

            slots = -1

            # Try job parameters first
            slots = int(job_params.get('numberOfSlots', slots))

            if slots == -1:
                # Try the cluster
                slots = int(cluster['config'].get('numberOfSlots', slots))

            parallel_env = _get_parallel_env(cluster, job)
            if parallel_env:
                job_params['parallelEnvironment'] = parallel_env

                # If the number of slots has not been provided we will get
                # the number of slots from the parallel environment
                if slots == -1:
                    slots = int(get_queue_adapter(cluster, conn)
                                .number_of_slots(parallel_env))
                    if slots > 0:
                        job_params['numberOfSlots'] = slots

            script = _generate_submission_script(job, cluster, job_params)

            conn.makedirs(job_dir)
            # put the script to master
            conn.put(StringIO(script), os.path.join(job_dir, script_name))

            if slots > -1:
                log.info('We have %s slots available' % slots)

            # Now submit the job
            queue_job_id \
                = get_queue_adapter(cluster, conn).submit_job(job,
                                                              script_name)

            # Update the state and queue job id
            job[AbstractQueueAdapter.QUEUE_JOB_ID] = queue_job_id
            patch_data = {
                'status': JobState.QUEUED,
                AbstractQueueAdapter.QUEUE_JOB_ID: queue_job_id,
                'dir': job_dir
            }

            r = requests.patch(status_url, headers=headers, json=patch_data)
            check_status(r)
            job = r.json()
            job['queuedTime'] = time.time()

            # Now monitor the jobs progress
            if monitor:
                monitor_job.s(
                    cluster, job, log_write_url=log_write_url,
                    girder_token=girder_token).apply_async(countdown=5)

        # Now update the status of the job
        headers = {'Girder-Token':  girder_token}
        r = requests.patch(status_url, headers=headers,
                           json={'status': JobState.QUEUED})
        check_status(r)
    except Exception as ex:
        traceback.print_exc()
        r = requests.patch(status_url, headers=headers,
                           json={'status': JobState.UNEXPECTEDERROR})
        check_status(r)
        get_job_logger(job, girder_token).exception(str(ex))
        raise


def submit(girder_token, cluster, job, log_url):
    # Do we inputs to download ?
    if 'input' in job and len(job['input']) > 0:

        download_job_input.delay(cluster, job, log_write_url=log_url,
                                 girder_token=girder_token)
    else:
        submit_job.delay(cluster, job, log_write_url=log_url,
                         girder_token=girder_token)


class JobState(object):
    CREATED = cumulus.constants.JobState.CREATED
    RUNNING = cumulus.constants.JobState.RUNNING
    TERMINATED = cumulus.constants.JobState.TERMINATED
    TERMINATING = cumulus.constants.JobState.TERMINATING
    UNEXPECTEDERROR = cumulus.constants.JobState.UNEXPECTEDERROR
    QUEUED = cumulus.constants.JobState.QUEUED
    ERROR = cumulus.constants.JobState.ERROR
    UPLOADING = cumulus.constants.JobState.UPLOADING
    ERROR_UPLOADING = cumulus.constants.JobState.ERROR_UPLOADING
    COMPLETE = cumulus.constants.JobState.COMPLETE

    def __init__(self, previous, **kwargs):
        if previous:
            for key in previous._keys:
                setattr(self, key, getattr(previous, key))
            self._keys = previous._keys
        else:
            self._keys = kwargs.keys()
            for key, value in kwargs.items():
                setattr(self, key, value)

    def __str__(self):
        return self.__class__.__name__.lower()

    def __lt__(self, other):
        return str(self) < str(other)

    def __cmp__(self, other):
        a = str(self)
        b = str(other)

        return (a > b) - (a < b)

    def __hash__(self):
        return hash(str(self))

    def _update_queue_time(self, job):
        if 'queuedTime' in job:
            queued_time = time.time() - job['queuedTime']
            job['timings'] = {'queued': int(round(queued_time * 1000))}
            del job['queuedTime']
            job['runningTime'] = time.time()

    def next(self, job_queue_status):
        raise NotImplementedError('Should be implemented by subclass')

    def run(self):
        raise NotImplementedError('Should be implemented by subclass')


class Created(JobState):
    def next(self, job_queue_status):
        if not job_queue_status or job_queue_status == JobQueueState.COMPLETE:
            return Uploading(self)
        elif job_queue_status == JobQueueState.RUNNING:
            return Running(self)
        elif job_queue_status == JobQueueState.QUEUED:
            return Queued(self)
        elif job_queue_status == JobQueueState.ERROR:
            return Error(self)
        else:
            raise Exception('Unrecognized state: %s' % job_queue_status)

    def run(self):
        return self


class Queued(JobState):
    def next(self, job_queue_status):
        if not job_queue_status or job_queue_status == JobQueueState.COMPLETE:
            return Uploading(self)
        elif job_queue_status == JobQueueState.RUNNING:
            return Running(self)
        elif job_queue_status == JobQueueState.QUEUED:
            return self
        elif job_queue_status == JobQueueState.ERROR:
            return Error(self)
        else:
            raise Exception('Unrecognized state: %s' % job_queue_status)

    def run(self):
        return self


class Running(JobState):
    def _tail_output(self):
        job_url = '%s/jobs/%s/log' % (cumulus.config.girder.baseUrl,
                                      self.job['_id'])
        log = get_post_logger(self.job['_id'], self.girder_token, job_url)

        # Do we need to tail any output files
        for output in self.job.get('output', []):
            if 'tail' in output and output['tail']:
                path = output['path']
                offset = 0
                if 'content' in output:
                    offset = len(output['content'])
                else:
                    output['content'] = []
                tail_path = os.path.join(self.job['dir'], path)
                command = 'tail -n +%d %s' % (offset, tail_path)
                try:
                    # Only tail if file exists
                    if self.conn.isfile(tail_path):
                        stdout = self.conn.execute(command)
                        output['content'] = output['content'] + stdout
                    else:
                        log.info('Skipping tail of %s as file doesn\'t '
                                 'currently exist' %
                                 tail_path)
                except Exception as ex:
                    get_job_logger(self.job,
                                   self.girder_token).exception(str(ex))

    def next(self, job_queue_status):
        if not job_queue_status or job_queue_status == JobQueueState.COMPLETE:
            return Uploading(self)
        elif job_queue_status == JobQueueState.RUNNING:
            return self
        elif job_queue_status == JobQueueState.ERROR:
            return Error(self)
        else:
            raise Exception('Unrecognized state: %s' % job_queue_status)

    def run(self):
        self._update_queue_time(self.job)
        self._tail_output()
        return self


class Complete(JobState):
    def next(self, job_queue_status):

        error = False
        for output in self.job.get('output', []):
            if 'errorRegEx' in output and output['errorRegEx']:
                stdout_file = '%s-%s.o%s' % (self.job['name'],
                                             self.job['_id'],
                                             self.job['queueJobId'])

                stderr_file = '%s-%s.o%s' % (self.job['name'],
                                             self.job['_id'],
                                             self.job['queueJobId'])
                variables = {
                    'stdout': stdout_file,
                    'stderr': stderr_file
                }

                tmp_path = None
                try:
                    path = Template(output['path']).render(**variables)
                    tmp_path = os.path.join(tempfile.tempdir, path)
                    path = os.path.join(self.job['dir'], path)
                    self.conn.get(path, localpath=tmp_path)
                    error_regex = re.compile(output['errorRegEx'])
                    with open(tmp_path, 'r') as fp:
                        for line in fp:
                            if error_regex.match(line):
                                error = True
                                break
                finally:
                    if tmp_path and os.path.exists(tmp_path):
                        os.remove(tmp_path)

            if error:
                break

        if error:
            return Error(self)

        return self

    def run(self):
        if _get_on_complete(self.job) == 'terminate':
            cluster_log_url = '%s/clusters/%s/log' % \
                (cumulus.config.girder.baseUrl, self.cluster['_id'])
            command.send_task(
                'cumulus.tasks.cluster.terminate_cluster',
                args=(self.cluster,),
                kwargs={'log_write_url': cluster_log_url,
                        'girder_token': self.girder_token})


class Terminating(JobState):
    def next(self, job_queue_status):
        if not job_queue_status or job_queue_status == JobQueueState.COMPLETE:
            return Terminated(self)
        else:
            return self

    def run(self):
        return self


class Terminated(JobState):
    def next(self, task, job, job_queue_status):
        return self

    def run(self):
        return self


class Uploading(JobState):
    def next(self, job_queue_status):
        job_url = '%s/jobs/%s/log' % (cumulus.config.girder.baseUrl,
                                      self.job['_id'])
        log = get_post_logger(self.job['_id'], self.girder_token, job_url)
        job_name = self.job['name']

        if 'runningTime' in self.job:
            running_time = time.time() - self.job['runningTime']
            self.job.get('timings', {})['running'] \
                = int(round(running_time * 1000))
            del self.job['runningTime']

        # Fire off task to upload the output
        log.info('Job "%s" complete' % job_name)

        upload = self.job.get('uploadOutput', True)

        if not upload or len(self.job.get('output', [])) == 0:
            return Complete(self)

        return self

    def run(self):
        upload = self.job.get('uploadOutput', True)

        if upload and len(self.job.get('output', [])) > 0:
            upload_job_output.delay(self.cluster, self.job,
                                    log_write_url=self.log_write_url,
                                    job_dir=self.job['dir'],
                                    girder_token=self.girder_token)


class Error(Complete):
    def next(self, job_queue_status):
        return self


class ErrorUploading(Uploading):
    def next(self, job_queue_status):
        return Error(self)

    def run(self):
        return self


class UnexpectedError(JobState):
    def next(self, job_queue_status):
        return self

    def run(self):
        return self


state_classes = {
    JobState.CREATED: Created,
    JobState.QUEUED: Queued,
    JobState.RUNNING: Running,
    JobState.COMPLETE: Complete,
    JobState.TERMINATING: Terminating,
    JobState.TERMINATED: Terminated,
    JobState.UPLOADING: Uploading,
    JobState.ERROR: Error,
    JobState.ERROR_UPLOADING: ErrorUploading,
    JobState.UNEXPECTEDERROR: UnexpectedError
}


def from_string(s, **kwargs):
    state = state_classes[s](None, **kwargs)
    return state


def _monitor_jobs(task, cluster, jobs, log_write_url=None, girder_token=None,
                  monitor_interval=5):
    headers = {'Girder-Token':  girder_token}

    cluster_url = '%s/clusters/%s' % (
        cumulus.config.girder.baseUrl, cluster['_id'])
    try:
        with get_connection(girder_token, cluster) as conn:

            try:
                job_queue_states \
                    = get_queue_adapter(cluster, conn).job_statuses(jobs)

                new_states = set()
                for (job, state) in job_queue_states:
                    job_id = job['_id']
                    # First get the current status
                    status_url = '%s/jobs/%s/status' % (
                        cumulus.config.girder.baseUrl, job_id)
                    r = requests.get(status_url, headers=headers)
                    check_status(r)
                    current_status = r.json()['status']

                    if current_status == JobState.TERMINATED:
                        continue

                    job_status = from_string(current_status, task=task,
                                             cluster=cluster, job=job,
                                             log_write_url=log_write_url,
                                             girder_token=girder_token,
                                             conn=conn)
                    job_status = job_status.next(state)
                    job['status'] = str(job_status)
                    job_status.run()
                    json = {
                        'status': str(job_status),
                        'timings': job.get('timings', {}),
                        'output': job['output']
                    }
                    job_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl,
                                              job['_id'])
                    r = requests.patch(job_url, headers=headers, json=json)
                    check_status(r)

                    new_states.add(job['status'])

                # Now see if we still have jobs to monitor
                running_states = set(
                    [JobState.CREATED, JobState.QUEUED,
                     JobState.RUNNING, JobState.TERMINATING]
                )

                # Do we have any job still in a running state?
                if new_states & running_states:
                    task.retry(countdown=monitor_interval)
            except EOFError:
                # Try again
                task.retry(countdown=5)
                return
            except paramiko.ssh_exception.NoValidConnectionsError:
                # Try again
                task.retry(countdown=5)
                return
    # Ensure that the Retry exception will get through
    except Retry:
        raise
    except paramiko.ssh_exception.NoValidConnectionsError as ex:
        r = requests.patch(cluster_url, headers=headers,
                           json={'status': 'error'})
        check_status(r)
        get_cluster_logger(cluster, girder_token).exception(str(ex))

    except Exception as ex:
        traceback.print_exc()
        r = requests.patch(cluster_url, headers=headers,
                           json={'status': 'error'})
        check_status(r)
        get_cluster_logger(cluster, girder_token).exception(str(ex))
        raise


@monitor.task(bind=True, max_retries=None, throws=(Retry,))
def monitor_job(task, cluster, job, log_write_url=None, girder_token=None,
                monitor_interval=5):
    _monitor_jobs(task, cluster, [job], log_write_url, girder_token,
                  monitor_interval=monitor_interval)


@monitor.task(bind=True, max_retries=None, throws=(Retry,))
def monitor_jobs(task, cluster, jobs, log_write_url=None, girder_token=None,
                 monitor_interval=5):
    _monitor_jobs(task, cluster, jobs, log_write_url, girder_token,
                  monitor_interval=monitor_interval)


def upload_job_output_to_item(cluster, job, log_write_url=None, job_dir=None,
                              girder_token=None):
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)

    try:
        # if terminating break out
        if _is_terminating(job, girder_token):
            return

        with get_connection(girder_token, cluster) as conn:
            # First put girder client on master
            path = inspect.getsourcefile(cumulus.girderclient)
            with open(path, 'r') as fp:
                conn.put(fp,
                         os.path.normpath(os.path.join(job_dir, '..',
                                                       os.path.basename(path))))

            cmds = ['cd %s' % job_dir]
            upload_cmd = 'python ../girderclient.py --token %s --url "%s" ' \
                         'upload --job %s' \
                         % (girder_token,
                            cumulus.config.girder.baseUrl, job['_id'])

            upload_output = '%s.upload.out' % job_id
            upload_output_path = os.path.normpath(os.path.join(job_dir, '..',
                                                               upload_output))
            cmds.append('nohup %s  &> ../%s  &\n' % (upload_cmd, upload_output))

            upload_cmd = _put_script(conn, '\n'.join(cmds))
            output = conn.execute(upload_cmd)

            # Remove upload script
            conn.remove(upload_cmd)

        if len(output) != 1:
            raise Exception('PID not returned by execute command')

        try:
            pid = int(output[0])
        except ValueError:
            raise Exception('Unable to extract PID from: %s' % output)

        on_complete = None

        if _get_on_complete(job) == 'terminate':
            cluster_log_url = '%s/clusters/%s/log' % \
                (cumulus.config.girder.baseUrl, cluster['_id'])
            on_complete = signature(
                'cumulus.tasks.cluster.terminate_cluster',
                args=(cluster,), kwargs={'log_write_url': cluster_log_url,
                                         'girder_token': girder_token})

        monitor_process.delay(cluster, job, pid, upload_output_path,
                              log_write_url=log_write_url,
                              on_complete=on_complete,
                              girder_token=girder_token)

    except Exception as ex:
        r = requests.patch(status_url, headers=headers,
                           json={'status': JobState.UNEXPECTEDERROR})
        check_status(r)
        get_job_logger(job, girder_token).exception(str(ex))


def upload_job_output_to_folder(cluster, job, log_write_url=None, job_dir=None,
                                girder_token=None):
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job['_id'])
    headers = {'Girder-Token':  girder_token}
    assetstore_base_url = get_assetstore_url_base(cluster)
    assetstore_id = get_assetstore_id(girder_token, cluster)
    if not job_dir:
        job_dir = job['dir']

    try:
        with get_connection(girder_token, cluster) as conn:
            for output in job['output']:
                if 'folderId' in output and 'path' in output:
                    folder_id = output['folderId']
                    path = os.path.join(job_dir, output['path'])
                    download_path(conn, girder_token, folder_id, path,
                                  assetstore_base_url, assetstore_id,
                                  include=output.get('include'),
                                  exclude=output.get('exclude'))
    except HttpError as e:
        job['status'] = JobState.ERROR
        url = '%s/jobs/%s/log' % (cumulus.config.girder.baseUrl, job['_id'])
        logger = get_post_logger('job', girder_token, url)
        logger.exception(e.responseText)
        r = requests.patch(status_url, headers=headers,
                           json={'status': JobState.ERROR})
        check_status(r)

    if _get_on_complete(job) == 'terminate':
        cluster_log_url = '%s/clusters/%s/log' % \
            (cumulus.config.girder.baseUrl, cluster['_id'])
        command.send_task(
            'cumulus.tasks.cluster.terminate_cluster',
            args=(cluster,), kwargs={'log_write_url': cluster_log_url,
                                     'girder_token': girder_token})

    # If we where uploading move job to the complete state
    if job['status'] == JobState.UPLOADING:
        job_status = from_string(job['status'], task=None,
                                 cluster=cluster, job=job,
                                 log_write_url=log_write_url,
                                 girder_token=girder_token,
                                 conn=conn)
        job_status = Complete(job_status)
        job_status = job_status.next(JobQueueState.COMPLETE)
        job_status.run()
        r = requests.patch(status_url, headers=headers,
                           json={'status': str(job_status)})
        check_status(r)


@command.task
def upload_job_output(cluster, job, log_write_url=None, job_dir=None,
                      girder_token=None):

    job_name = job['name']
    job_url = '%s/jobs/%s/log' % (cumulus.config.girder.baseUrl, job['_id'])
    log = get_post_logger(job['_id'], girder_token, job_url)

    log.info('Uploading output for "%s"' % job_name)

    if parse('output.itemId').find(job):
        upload_job_output_to_item(cluster, job, log_write_url=log_write_url,
                                  job_dir=job_dir, girder_token=girder_token)
    else:
        upload_job_output_to_folder(cluster, job, log_write_url=log_write_url,
                                    job_dir=job_dir, girder_token=girder_token)


@monitor.task(bind=True, max_retries=None)
def monitor_process(task, cluster, job, pid, nohup_out_path,
                    log_write_url=None, on_complete=None,
                    output_message='Job download/upload error: %s',
                    girder_token=None):
    job_url = '%s/jobs/%s/log' % (cumulus.config.girder.baseUrl, job['_id'])
    log = get_post_logger(job['_id'], girder_token, job_url)
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)

    try:
        # if terminating break out
        if _is_terminating(job, girder_token):
            return

        with get_connection(girder_token, cluster) as conn:
            # See if the process is still running
            output = conn.execute('ps %s | grep %s' % (pid, pid),
                                  ignore_exit_status=True,
                                  source_profile=False)

            if len(output) > 0:
                # Process is still running so schedule self again in about 5
                # secs
                # N.B. throw=False to prevent Retry exception being raised
                task.retry(throw=False, countdown=5)
            else:
                try:
                    nohup_out_file_name = os.path.basename(nohup_out_path)

                    # Log the output
                    with conn.get(nohup_out_path) as fp:
                        output = fp.read()
                        if output.strip():
                            log.error(output_message % output)
                            # If we have output then set the error state on the
                            # job and return
                            r = requests.patch(status_url, headers=headers,
                                               json={'status': JobState.ERROR})
                            check_status(r)
                            return
                finally:
                    if nohup_out_file_name and \
                       os.path.exists(nohup_out_file_name):
                        os.remove(nohup_out_file_name)

                # Fire off the on_compete task if we have one
                if on_complete:
                    signature(on_complete).delay()

                # If we where uploading move job to the complete state
                if job['status'] == JobState.UPLOADING:
                    job_status = from_string(job['status'], task=task,
                                             cluster=cluster, job=job,
                                             log_write_url=log_write_url,
                                             girder_token=girder_token,
                                             conn=conn)
                    job_status = Complete(job_status)
                    job_status = job_status.next(JobQueueState.COMPLETE)
                    job_status.run()
                    r = requests.patch(status_url, headers=headers,
                                       json={'status': str(job_status)})
                    check_status(r)

    except EOFError:
        # Try again
        task.retry(throw=False, countdown=5)
    except Exception as ex:
        r = requests.patch(status_url, headers=headers,
                           json={'status': JobState.UNEXPECTEDERROR})
        check_status(r)
        get_job_logger(job, girder_token).exception(str(ex))
        raise


@command.task
def terminate_job(cluster, job, log_write_url=None, girder_token=None):
    script_filepath = None
    headers = {'Girder-Token':  girder_token}
    job_id = job['_id']
    status_url = '%s/jobs/%s' % (cumulus.config.girder.baseUrl, job_id)

    try:

        with get_connection(girder_token, cluster) as conn:
            if AbstractQueueAdapter.QUEUE_JOB_ID in job:
                queue_adapter = get_queue_adapter(cluster, conn)
                output = queue_adapter.terminate_job(job)
            else:
                r = requests.patch(status_url, headers=headers,
                                   json={'status': JobState.TERMINATED})
                check_status(r)

            if 'onTerminate' in job:
                commands = '\n'.join(job['onTerminate']['commands']) + '\n'
                commands = Template(commands) \
                    .render(cluster=cluster,
                            job=job,
                            base_url=cumulus.config.girder.baseUrl)

                on_terminate = _put_script(conn, commands + '\n')

                terminate_output = '%s.terminate.out' % job_id
                terminate_cmd = 'nohup %s  &> %s  &\n' % (on_terminate,
                                                          terminate_output)
                terminate_cmd = _put_script(conn, terminate_cmd)
                output = conn.execute(terminate_cmd)

                conn.remove(on_terminate)
                conn.remove(terminate_cmd)

                if len(output) != 1:
                    raise Exception('PID not returned by execute command')

                try:
                    pid = int(output[0])
                except ValueError:
                    raise Exception('Unable to extract PID from: %s'
                                    % output)

                output_message = 'onTerminate error: %s'
                monitor_process.delay(cluster, job, pid, terminate_output,
                                      log_write_url=log_write_url,
                                      output_message=output_message,
                                      girder_token=girder_token)

    except Exception as ex:
        r = requests.patch(status_url, headers=headers,
                           json={'status': JobState.UNEXPECTEDERROR})
        check_status(r)
        get_job_logger(job, girder_token).exception(str(ex))
        raise
    finally:
        if script_filepath and os.path.exists(script_filepath):
            os.remove(script_filepath)


@command.task(bind=True, max_retries=5)
def remove_output(task, cluster, job, girder_token):
    try:
        with get_connection(girder_token, cluster) as conn:
            rm_cmd = 'rm -rf %s' % job['dir']
            conn.execute(rm_cmd)
    except EOFError:
        # Try again
        task.retry(countdown=5)
