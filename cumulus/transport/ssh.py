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
from jsonpath_rw import parse

from .abstract import AbstractConnection
import cumulus

from paramiko.client import SSHClient
from paramiko import RSAKey
import paramiko


class SshCommandException(Exception):
    def __init__(self, command, exit_code, output):
        super(SshCommandException, self).__init__('"%s" failed with %s'
                                                  % (command, exit_code))
        self.command = command
        self.exit_code = exit_code
        self.output = output


class SshClusterConnection(AbstractConnection):
    def __init__(self, girder_token, cluster):
        self._girder_token = girder_token
        self._cluster = cluster

    def _load_rsa_key(self, path, passphrase):
        return RSAKey.from_private_key_file(path, password=passphrase)

    def __enter__(self):
        self._client = SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        username = parse('config.ssh.user').find(self._cluster)[0].value
        hostname = parse('config.host').find(self._cluster)[0].value

        port = parse('config.port').find(self._cluster)
        if port:
            port = port[0].value
        else:
            port = 22

        passphrase \
            = parse('config.ssh.passphrase').find(self._cluster)
        if passphrase:
            passphrase = passphrase[0].value
        else:
            passphrase = None

        key_name = parse('config.ssh.key').find(self._cluster)[0].value
        key_path = os.path.join(cumulus.config.ssh.keyStore,
                                key_name)

        private_key = self._load_rsa_key(key_path, passphrase)

        self._client.connect(hostname=hostname, port=port,
                             username=username, pkey=private_key)

        return self

    def __exit__(self, type, value, traceback):
        self._client.close()

    def execute(self, command, ignore_exit_status=False, source_profile=True):
        if source_profile:
            command = 'source /etc/profile && %s' % command

        chan = self._client.get_transport().open_session()
        chan.exec_command(command)
        stdout = chan.makefile('r', -1)
        stderr = chan.makefile_stderr('r', -1)

        output = stdout.readlines() + stderr.readlines()
        exit_code = chan.recv_exit_status()
        if ignore_exit_status and exit_code != 0:
            raise SshCommandException(command, exit_code, output)

        return output

    @contextmanager
    def get(self, remote_path):
        sftp = None
        file = None
        try:
            sftp = self._client.get_transport().open_sftp_client()
            file = sftp.open(remote_path)
            yield file
        finally:
            if file:
                file.close()
                sftp.close()

    def isfile(self, remote_path):

        with self._client.get_transport().open_sftp_client() as sftp:
            try:
                s = sftp.stat(remote_path)
            except IOError:
                return False

            return stat.S_ISDIR(s.st_mode)

    def mkdir(self, remote_path, ignore_failure=False):
        with self._client.get_transport().open_sftp_client() as sftp:
            try:
                sftp.mkdir(remote_path)
            except IOError:
                if not ignore_failure:
                    raise

    def makedirs(self, remote_path):
        with self._client.get_transport().open_sftp_client() as sftp:
            current_path = ''
            if remote_path[0] == '/':
                current_path = '/'

            for path in remote_path.split('/'):
                if not path:
                    continue
                current_path = os.path.join(current_path, path)
                try:
                    sftp.listdir(current_path)
                except IOError:
                    sftp.mkdir(current_path)

    def put(self, stream, remote_path):
        with self._client.get_transport().open_sftp_client() as sftp:
            sftp.putfo(stream, remote_path)

    def stat(self, remote_path):
        with self._client.get_transport().open_sftp_client() as sftp:
            return sftp.stat(remote_path)

    def remove(self, remote_path):
        with self._client.get_transport().open_sftp_client() as sftp:
            return sftp.remove(remote_path)

    def list(self, remote_path):
        with self._client.get_transport().open_sftp_client() as sftp:
            for path in sftp.listdir_iter(remote_path):
                yield {
                    'name': path.filename,
                    'user': path.st_uid,
                    'group': path.st_gid,
                    'mode': path.st_mode,
                    # For now just pass mtime through
                    'date': path.st_mtime,
                    'size': path.st_size
                }
