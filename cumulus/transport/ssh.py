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

from starcluster.sshutils import SSHClient
import starcluster.config
from jsonpath_rw import parse

from .abstract import AbstractConnection
from cumulus.constants import ClusterType
import cumulus
from cumulus.common import create_config_request


class SshClusterConnection(AbstractConnection):
    def __init__(self, girder_token, cluster):
        self._girder_token = girder_token
        self._cluster = cluster

    def __enter__(self):
        if self._cluster['type'] == ClusterType.TRADITIONAL:
            username = parse('config.ssh.user').find(self._cluster)[0].value
            hostname = parse('config.host').find(self._cluster)[0].value
            passphrase \
                = parse('config.ssh.passphrase').find(self._cluster)[0].value

            key_path = os.path.join(cumulus.config.ssh.keyStore,
                                    self._cluster['_id'])

            self._conn = SSHClient(host=hostname, username=username,
                                   private_key=key_path,
                                   private_key_pass=passphrase, timeout=5)
            self._conn.connect()
        else:
            cluster_id = self._cluster['_id']
            config_id = self._cluster['config']['_id']
            config_request = create_config_request(
                self._girder_token, cumulus.config.girder.baseUrl, config_id)
            config = starcluster.config.StarClusterConfig(config_request)

            config.load()
            cm = config.get_cluster_manager()
            sc = cm.get_cluster(cluster_id)
            master = sc.master_node
            master.user = sc.cluster_user
            self._conn = master.ssh

        return self

    def __exit__(self, type, value, traceback):
        self._conn.close()

    def execute(self, command, ignore_exit_status=False, source_profile=True):
        return self._conn.execute(command,
                                  ignore_exit_status=ignore_exit_status,
                                  source_profile=source_profile)

    @contextmanager
    def get(self, remote_path):
        sftp = None
        file = None
        try:
            sftp = self._conn.transport.open_sftp_client()
            file = sftp.open(remote_path)
            yield file
        finally:
            if file:
                file.close()
                sftp.close()

    def isfile(self, remote_path):

        with self._conn.transport.open_sftp_client() as sftp:
            try:
                s = sftp.stat(remote_path)
            except IOError:
                return False

            return stat.S_ISDIR(s.st_mode)

    def mkdir(self, remote_path, ignore_failure=False):
        with self._conn.transport.open_sftp_client() as sftp:
            try:
                sftp.mkdir(remote_path)
            except IOError:
                if not ignore_failure:
                    raise

    def put(self, stream, remote_path):
        with self._conn.transport.open_sftp_client() as sftp:
            sftp.putfo(stream, remote_path)

    def stat(self, remote_path):
        with self._conn.transport.open_sftp_client() as sftp:
            return sftp.stat(remote_path)

    def remove(self, remote_path):
        with self._conn.transport.open_sftp_client() as sftp:
            return sftp.remove(remote_path)

    def list(self, remote_path):
        with self._conn.transport.open_sftp_client() as sftp:
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
