###############################################################################
#  Copyright 2016 Kitware Inc.
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

import pkg_resources as pr
import os
import subprocess
import json
import select
import cumulus
import requests
from cumulus.common import check_status
from celery.utils.log import get_task_logger
from cumulus.ssh.tasks.key import _key_path

logger = get_task_logger(__name__)


def get_playbook_directory():
    return pr.resource_filename('cumulus', 'ansible/tasks/playbooks')


def get_playbook_path(name):
    return os.path.join(get_playbook_directory(), name + '.yml')


def get_callback_plugins_path():
    return os.path.join(get_playbook_directory(),
                        'callback_plugins')


def get_library_path():
    return os.path.join(get_playbook_directory(),
                        'library')


def run_playbook(playbook, inventory, extra_vars=None,
                 verbose=None, env=None):

    env = env if env is not None else os.environ.copy()

    cmd = ['ansible-playbook', '-i', inventory]

    if verbose is not None:
        cmd.append('-%s' % ('v' * verbose))

    if extra_vars is not None:
        cmd.extend(['--extra-vars', json.dumps(extra_vars)])

    cmd.append(playbook)

    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    while True:
        reads = [p.stdout.fileno(), p.stderr.fileno()]
        ret = select.select(reads, [], [])

        for fd in ret[0]:
            if fd == p.stdout.fileno():
                logger.info(p.stdout.readline())
            if fd == p.stderr.fileno():
                err = p.stderr.readline()
                # Ansible produces a number of empty stderr lines
                # Make sure we don't report these as errors
                if err.strip() != '':
                    logger.error(err)

        if p.poll() is not None:
            return p.wait()


def get_playbook_variables(cluster, profile, extra_vars):
    # Default variables all playbooks will need
    playbook_variables = {
        'cluster_region': profile['regionName'],
        'cluster_zone': profile['availabilityZone'],
        'cluster_id': cluster['_id'],
        'ansible_ssh_private_key_file': _key_path(profile)
    }

    # Update with variables passed in from the cluster adapater
    playbook_variables.update(extra_vars)

    # If no keyname is provided use the one associated with the profile
    if 'aws_keyname' not in playbook_variables:
        playbook_variables['aws_keyname'] = profile['_id']

    return playbook_variables


def check_girder_cluster_status(cluster, girder_token, post_status):
    # Check status from girder
    cluster_id = cluster['_id']
    headers = {'Girder-Token':  girder_token}
    status_url = '%s/clusters/%s/status' % (cumulus.config.girder.baseUrl,
                                            cluster_id)
    r = requests.get(status_url, headers=headers)
    status = r.json()['status']

    if status != 'error':
        # Update girder with the new status
        status_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl,
                                         cluster_id)
        updates = {
            'status': post_status
        }

        r = requests.patch(status_url, headers=headers, json=updates)
        check_status(r)


def check_ansible_return_code(returncode, cluster, girder_token):
    if returncode != 0:
        check_status(requests.patch('%s/clusters/%s' %
                                    (cumulus.config.girder.baseUrl,
                                     cluster['_id']),
                                    headers={'Girder-Token': girder_token},
                                    json={'status': 'error'}))
