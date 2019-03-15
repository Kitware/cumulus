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

import cherrypy
import os
import paramiko
import stat
from contextlib import contextmanager

from girder.models.model_base import ValidationException
from girder.utility.abstract_assetstore_adapter import AbstractAssetstoreAdapter
from girder.utility.model_importer import ModelImporter
from girder import events
from girder.api.rest import RestException

from paramiko.ssh_exception import SSHException


BUFFER_SIZE = 32768

class SftpAssetstoreAdapter(AbstractAssetstoreAdapter):
    def __init__(self, assetstore):
        self.assetstore = assetstore
        self.host = assetstore['sftp']['host']
        self.user = assetstore['sftp']['user']
        self.authKey = assetstore['sftp'].get('authKey')

    @staticmethod
    def validateInfo(doc):
        """
        Ensures we have the necessary information.
        """
        info = doc.get('sftp', {})
        for field in ('host', 'user'):
            if field not in info:
                raise ValidationException('Missing %s field.' % field)

        return doc

    def _get_credentials(self):
        private_key=None
        private_key_pass=None

        info  = {
            'user': self.user,
            'assetstoreId': self.assetstore['_id'],
            'authKey': self.authKey
        }
        e = events.trigger('assetstore.sftp.credentials.get', info=info)

        if len(e.responses) > 0:
            (private_key, private_key_pass) = e.responses[-1]

        return (private_key, private_key_pass)

    @contextmanager
    def open_ssh_connection(self):
        ssh = None

        (private_key, private_key_pass) = self._get_credentials()

        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=self.host, username=self.user,
                        key_filename=private_key,
                        password=private_key_pass)

            yield ssh
        finally:
            if ssh:
                ssh.close()

    def downloadFile(self, file, offset=0, headers=True, end_byte=None,
                     **kwargs):
        path = file['path']
        if end_byte is None or end_byte > file['size']:
            end_byte = file['size']

        if headers:
            cherrypy.response.headers['Accept-Ranges'] = 'bytes'
            self.setContentHeaders(file, offset, end_byte)

        def stream():
            bytes_read = offset

            with self.open_ssh_connection() as ssh:
                with ssh.open_sftp() as sftp_client:
                    with sftp_client.open(path,
                                      mode='r', bufsize=-1) as sftp_file:

                        # If we are fetching the whole file then use prefetch
                        if offset == 0 and end_byte == file['size']:
                            sftp_file.prefetch(file['size'])

                        if offset > 0:
                            sftp_file.seek(offset)

                        while True:
                            read_len = min(BUFFER_SIZE, end_byte - bytes_read)
                            if read_len <= 0:
                                break

                            data = sftp_file.read(read_len)
                            bytes_read += read_len

                            if not data:
                                break
                            yield data

        return stream

    def _import_path(self, parent, user, path, parent_type='folder', ssh=None):
        with ssh.open_sftp() as sftp_client:
            for p in sftp_client.listdir_iter(path=path):
                name  = p.filename

                full_path = os.path.join(path, name)
                if name in ['.', '..']:
                    continue
                if stat.S_ISDIR(p.st_mode):
                    folder = ModelImporter.model('folder').createFolder(
                        parent=parent, name=name, parentType=parent_type,
                        creator=user, reuseExisting=True)

                    self._import_path(folder, user, full_path, parent_type='folder', ssh=ssh)
                else:
                    size = p.st_size
                    item = ModelImporter.model('item').createItem(
                        name=name, creator=user, folder=parent, reuseExisting=True)
                    file = ModelImporter.model('file').createFile(
                        name=name, creator=user, item=item, reuseExisting=True,
                        assetstore=self.assetstore, mimeType=None, size=size)
                    file['imported'] = True
                    file['path'] = full_path
                    ModelImporter.model('file').save(file)

    def importData(self, parent, parentType, params, progress, user, **kwargs):
        import_path = params.get('importPath', '').strip()

        if import_path and import_path[0] != '/':
            import_path = '/%s' % import_path

        with self.open_ssh_connection() as ssh:
            self._import_path(parent, user, import_path, parent_type=parentType,
                              ssh=ssh)

    def deleteFile(self, file):
        """
        This assetstore is read-only.
        """
        pass

    def initUpload(self, upload):
        raise NotImplementedError('Read-only, unsupported operation')

    def uploadChunk(self, upload, chunk):
        raise NotImplementedError('Read-only, unsupported operation')

    def finalizeUpload(self, upload, file):
        raise NotImplementedError('Read-only, unsupported operation')

    def cancelUpload(self, upload):
        raise NotImplementedError('Read-only, unsupported operation')

    def requestOffset(self, upload):
        raise NotImplementedError('Read-only, unsupported operation')
