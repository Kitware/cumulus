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

from __future__ import print_function
import requests
import os
import json
import argparse
import sys
import zipfile
import tempfile
import shutil
import errno
import time
import re

max_chunk_size = 1024 * 1024 * 64


class GirderBase(object):

    def __init__(self, girder_token):
        self._girder_token = girder_token
        self._headers = {'Girder-Token': self._girder_token}

    def check_status(self, request):
        if request.status_code != 200:
            if request.headers['Content-Type'] == 'application/json':
                print(request.json(), file=sys.stderr)
            request.raise_for_status()


class DirectoryUploader(GirderBase):

    def __init__(self, girder_token, base_url, job_id):
        self._dir = dir
        self._base_url = base_url
        self._job_id = job_id
        super(DirectoryUploader, self).__init__(girder_token)

    def run(self):
        job_url = '%s/jobs/%s' % (self._base_url, self._job_id)

        r = requests.get(job_url, headers=self._headers)
        self.check_status(r)

        job = r.json()

        start = time.time()

        for i in job['output']:

            if 'itemId' in i and 'path' in i:
                item_id = i['itemId']
                path_spec = i['path']
                exclude_regex = i.get('exclude', None)
                self._upload(item_id, path_spec, exclude_regex=exclude_regex)

        end = time.time()

        upload_time = end - start

        updates = {
            'timings': {
                'upload': int(round(upload_time * 1000))
            }
        }

        r = requests.patch(job_url, json=updates, headers=self._headers)
        self.check_status(r)

    def _upload_file(self, name, path, parent_id):
        datalen = os.path.getsize(path)

        params = {
            'parentType': 'item',
            'parentId': parent_id,
            'name': name,
            'size': datalen
        }

        r = requests.post(
            '%s/file' % self._base_url, params=params, headers=self._headers)
        self.check_status(r)
        obj = r.json()

        if '_id' in obj:
            upload_id = obj['_id']
        else:
            raise Exception('Unexpected response: ' + json.dumps(obj))

        uploaded = 0

        with open(path, 'rb') as fp:
            while (uploaded != datalen):

                chunk_size = datalen - uploaded
                if chunk_size > max_chunk_size:
                    chunk_size = max_chunk_size

                part = fp.read(chunk_size)

                params['uploadId'] = upload_id
                params['offset'] = uploaded
                headers = self._headers.copy()
                headers['Content-Type'] = 'application/octet-stream'

                r = requests.post('%s/file/chunk' % self._base_url,
                                  params=params, data=part, headers=headers)
                self.check_status(r)

                uploaded += chunk_size

    def _upload(self, parent_id, path, exclude_regex=None):
        if os.path.isdir(path):
            for root, _, file_list in os.walk(path):
                for filename in file_list:
                    file_path = os.path.join(root, filename)

                    name = file_path
                    if not path.startswith('/'):
                        name = os.path.relpath(file_path, os.getcwd())

                    if exclude_regex and re.compile(exclude_regex).match(name):
                        continue

                    self._upload_file(name, file_path, parent_id)
        else:
            self._upload_file(path, path, parent_id)


class JobInputDownloader(GirderBase):

    def __init__(self, girder_token, base_url, job_id, dest):
        self._girder_token = girder_token
        self._base_url = base_url
        self._job_id = job_id
        self._dest = dest
        super(JobInputDownloader, self).__init__(girder_token)

    def _mkdir(self, path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def _download_item(self, item_id, target_path):

        item_files_url = '%s/item/%s/files' % (self._base_url, item_id)
        r = requests.get(item_files_url, headers=self._headers)
        self.check_status(r)

        files = r.json()

        if len(files) == 1:
            item_url = '%s/item/%s/download' % (self._base_url, item_id)
            r = requests.get(item_url, headers=self._headers)
            self.check_status(r)
            dest_path = os.path.join(self._dest, target_path, files[0]['name'])

            self._mkdir(os.path.dirname(dest_path))
            with open(dest_path, 'w') as fp:
                fp.write(r.content)
        elif len(files) > 1:
            # Download the item in zip format
            item_url = '%s/item/%s/download' % (self._base_url, item_id)
            r = requests.get(item_url, headers=self._headers, stream=True)
            self.check_status(r)
            with tempfile.NamedTemporaryFile(suffix='.zip') as fp:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        fp.write(chunk)

                fp.flush()
                with zipfile.ZipFile(fp.name, 'r') as files:
                    dest_path = os.path.join(self._dest, target_path)

                    temp_dir = None
                    try:
                        temp_dir = tempfile.mkdtemp()
                        files.extractall(temp_dir)
                        item_dir = os.listdir(temp_dir)

                        if len(item_dir) != 1:
                            raise Exception(
                                'Expecting single item directory, got: %s'
                                % len(item_dir))

                        item_dir = os.path.join(temp_dir, item_dir[0])
                        if not item_dir.endswith('/'):
                            item_dir += '/'

                        for dir, subdirs, files in os.walk(item_dir):
                            for f in files:
                                src = os.path.join(item_dir, dir, f)
                                dest = os.path.join(
                                    dest_path, dir.replace(item_dir, ''), f)
                                self._mkdir(os.path.dirname(dest))
                                shutil.copy(src, dest)
                    finally:
                        if os.path.exists(temp_dir):
                            shutil.rmtree(temp_dir)

    def run(self):
        job_url = '%s/jobs/%s' % (self._base_url, self._job_id)

        r = requests.get(job_url, headers=self._headers)
        self.check_status(r)

        job = r.json()

        start = time.time()

        for i in job['input']:
            item_id = i['itemId']
            target_path = i['path']
            self._download_item(item_id, target_path)

        end = time.time()

        download_time = end - start

        updates = {
            'timings': {
                'download': int(round(download_time * 1000))
            }
        }

        r = requests.patch(job_url, json=updates, headers=self._headers)
        self.check_status(r)


def main():
    parser = argparse.ArgumentParser(
        description='Girder client for download/upload')

    # Common arguments
    parser.add_argument(
        '--token', help='The Grider token to use when access server',
        required=True)
    parser.add_argument('--url', help='Base URL for Girder ops', required=True)

    subparsers = parser.add_subparsers(title='actions', dest='action')

    # Upload
    upload_parser = subparsers.add_parser(
        'upload', help='Upload paths to girder items')
    upload_parser.add_argument(
        '--job', help='The job to upload output for', required=True)

    # Download
    download_parser = subparsers.add_parser(
        'download', help='Download file from a Girder item')
    download_parser.add_argument(
        '--job', help='The job to download input for', required=True)
    download_parser.add_argument(
        '--dir', help='The target directory', required=True)

    config = parser.parse_args()

    if config.action == 'upload':
        DirectoryUploader(config.token, config.url, config.job).run()
    elif config.action == 'download':
        JobInputDownloader(
            config.token, config.url, config.job, config.dir).run()


if __name__ == '__main__':
    main()
