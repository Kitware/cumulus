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

import os
from contextlib import contextmanager
import stat
import re
import six

import requests
from paramiko import SFTPAttributes

from jsonpath_rw import parse

from .abstract import AbstractConnection
import cumulus
from cumulus.common import check_status

NEWT_BASE_URL = 'https://newt.nersc.gov/newt'

newt_stat_command = '/bin/stat -c "st_mode=%f,st_ino=%i,st_dev=%d,' \
    'st_nlink=%h,st_uid=%u,st_gid=%g,st_size=%s,st_atime=%X,st_mtime=%Y,' \
    'st_ctime=%Z" '
newt_mkdir_path = '/bin/mkdir'
newt_rm_path = '/bin/rm'

commands = {
    'ls': '/bin/ls',
    'rm': '/bin/rm',
    'pwd': '/bin/pwd',
    'tail': '/usr/bin/tail',
    # This may be very machine dependant!
    'squeue': '/opt/slurm/default/bin/squeue'
}

type = {
    'd': stat.S_IFDIR,
    'l': stat.S_IFLNK
}

user = {
    'r': stat.S_IRUSR,
    'w': stat.S_IWUSR,
    'x': stat.S_IXUSR
}

group = {
    'r': stat.S_IRGRP,
    'w': stat.S_IWGRP,
    'x': stat.S_IXGRP
}

other = {
    'r': stat.S_IROTH,
    'w': stat.S_IWOTH,
    'x': stat.S_IXOTH
}


class NewtException(Exception):
    pass


class NewtClusterConnection(AbstractConnection):
    def __init__(self, girder_token, cluster):
        self._girder_token = girder_token
        self._cluster = cluster
        self._newt_session_id = None
        self._machine = parse('config.host').find(cluster)[0].value

    def __enter__(self):

        # Do we need to get the session id for this user
        if not self._newt_session_id:
            headers = {'Girder-Token':  self._girder_token}
            url = '%s/newt/sessionId' % cumulus.config.girder.baseUrl
            r = requests.get(url, headers=headers)
            check_status(r)

            session_id = parse('sessionId').find(r.json())

            if not session_id:
                raise Exception('No NEWT session ID present')

            self._session = requests.Session()
            self._newt_session_id = session_id[0].value
            self._session.cookies.set('newt_sessionid', self._newt_session_id)

        return self

    def __exit__(self, type, value, traceback):
        pass

    def execute(self, command, ignore_exit_status=False, source_profile=True):
        url = '%s/command/%s' % (NEWT_BASE_URL, self._machine)

        # NEWT requires all commands are issued using a full executable path
        for (name, full_path) in six.iteritems(commands):
            command = re.sub(r'^%s[ ]*' % name, '%s ' % full_path, command)

        data = {
            'executable': command,
            'loginenv': source_profile
        }

        r = self._session.post(url, data=data)
        check_status(r)

        json_response = r.json()
        if json_response['error']:
            raise NewtException(json_response['error'])

        return json_response['output'].split('\n')

    @contextmanager
    def get(self, remote_path):
        url = '%s/file/%s/%s' % (NEWT_BASE_URL, self._machine, remote_path)
        params = {
            'view': 'read'
        }
        r = None

        try:
            r = self._session.get(url, params=params, stream=True)
            check_status(r)

            yield r.raw
        finally:
            if r:
                r.close()

    def isfile(self, remote_path):
        try:
            s = self.stat(remote_path)
        except NewtException:
            return False

        return not stat.S_ISDIR(s.st_mode)

    # TODO remote_path probably needs to be a full path
    def mkdir(self, remote_path, ignore_failure=False):
        command = newt_mkdir_path

        try:
            command += ' %s' % remote_path
            return self.execute(command)
        except Exception:
            if not ignore_failure:
                raise

    def makedirs(self, remote_path):
        command = newt_mkdir_path
        command += ' -p %s' % remote_path

        return self.execute(command)

    def _home_dir(self):
        home = self.execute('pwd')[0]

        return home

    def put(self, stream, remote_path):

        name = os.path.basename(remote_path)
        path = os.path.dirname(remote_path)

        # If not a full path then assume relative to users home
        if path[0] != '/':
            # Get the users home directory
            path = os.path.abspath(os.path.join(self._home_dir(), path))

        files = {
            'file': (name, stream)
        }
        url = '%s/file/%s%s' % (NEWT_BASE_URL, self._machine, path)
        r = self._session.post(url, files=files)
        check_status(r)

    def stat(self, remote_path):
        output = self.execute(newt_stat_command + remote_path)[0]
        values = dict(s.split('=') for s in output.split(','))
        attributes = SFTPAttributes()
        for (key, value) in six.iteritems(values):
            try:
                value = int(value)
            except ValueError:
                value = int(value, 16)

            setattr(attributes, key, value)

        return attributes

    def remove(self, remote_path):
        command = newt_rm_path + ' %s' % remote_path

        return self.execute(command)

    def _perms_to_mode(self, perms):
        mode = 0
        index = 0

        def apply_perms(perms_to_modes, perms):
            mode = 0
            for p in perms:
                if p in perms_to_modes:
                    mode |= perms_to_modes[p]

            return mode

        # type
        mode |= apply_perms(type, perms[index:1])
        index += 1

        # user
        mode |= apply_perms(user, perms[index: index+3])
        index += 3

        # group
        mode |= apply_perms(group, perms[index: index+3])
        index += 3

        # other
        mode |= apply_perms(other, perms[index: index+3])
        index += 3

        return mode

    def list(self, remote_path):
        if remote_path[0] != '/':
            # Get the users home directory
            remote_path = os.path.abspath(os.path.join(self._home_dir(),
                                                       remote_path))

        url = '%s/file/%s/%s' % (NEWT_BASE_URL, self._machine, remote_path)
        r = self._session.get(url)
        check_status(r)

        paths = r.json()

        for path in paths:
            perms = path['perms']
            del path['perms']
            del path['hardlinks']

            path['mode'] = self._perms_to_mode(perms)
            path['size'] = int(path['size'])
            yield path

    @property
    def session_id(self):
        """
        Allow access to session id, this is used in the queue adapter
        """
        return self._newt_session_id
