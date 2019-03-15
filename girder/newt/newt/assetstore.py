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
import requests
from jsonpath_rw import parse

from girder.exceptions import GirderException
from girder.models.model_base import ValidationException
from girder.utility.abstract_assetstore_adapter import AbstractAssetstoreAdapter
from girder.utility.model_importer import ModelImporter
from girder.api.rest import getCurrentUser

BUF_LEN = 65536


class NewtAssetstoreAdapter(AbstractAssetstoreAdapter):
    def __init__(self, assetstore):
        self.assetstore = assetstore
        self.machine = assetstore['newt']['machine']
        self.newt_base_url = assetstore['newt']['baseUrl'].rstrip('/')

    @staticmethod
    def validateInfo(doc):
        """
        Ensures we have the necessary information.
        """
        info = doc.get('newt', {})
        for field in ('machine', 'baseUrl'):
            if field not in info:
                raise ValidationException('Missing %s field.' % field)

        return doc

    def downloadFile(self, file, offset=0, headers=True, endByte=None,
                     **kwargs):

        if 'path' not in file:
            raise Exception('Missing path property')

        full_path = file['path']
        url = '%s/file/%s/%s?view=read' % (self.newt_base_url, self.machine, full_path)
        if headers:
            raise cherrypy.HTTPRedirect(url)
        else:
            session_id = parse('newt.sessionId').find(getCurrentUser())
            if len(session_id) > 0:
                session_id = session_id[0].value

            if session_id is None:
                raise GirderException('Missing NEWT session id')

            def stream():
                cookies = dict(newt_sessionid=session_id)
                r = requests.get(url, cookies=cookies, stream=True)
                for chunk in r.iter_content(chunk_size=BUF_LEN):
                    if chunk:
                        yield chunk
            return stream

    def _import_path(self, parent, user, path, parent_type='folder'):
        url = '%s/file/%s/%s' % (self.newt_base_url, self.machine, path)

        if 'newt_sessionid' not in cherrypy.request.cookie:
            raise GirderException('Missing newt_sessionid')

        newt_sessionid = cherrypy.request.cookie['newt_sessionid'].value
        cookies = dict(newt_sessionid=newt_sessionid)
        r = requests.get(url, cookies=cookies)
        r.raise_for_status()

        paths = r.json()

        for p in paths:
            perms = p['perms']
            name  = p['name']
            size = int(p['size'])

            full_path = os.path.join(path, name)
            if name in ['.', '..']:
                continue

            if perms.startswith('d'):
                print
                folder = ModelImporter.model('folder').createFolder(
                    parent=parent, name=name, parentType=parent_type,
                    creator=user, reuseExisting=True)

                self._import_path(folder, user, full_path)
            else:
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

        self._import_path(parent, user, import_path, parent_type=parentType)

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
