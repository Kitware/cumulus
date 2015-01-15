import requests
from requests_toolbelt import MultipartEncoder
import os
import json
import argparse
import sys
import io
import zipfile
import tempfile
import shutil
import os
import errno

max_chunk_size = 1024 * 1024 * 64

class GirderBase(object):
    def __init__(self, girder_token):
        self._girder_token = girder_token
        self._headers = {'Girder-Token': self._girder_token}

    def _check_status(self, request):
        if request.status_code != 200:
            if request.headers['Content-Type'] == 'application/json':
                print >> sys.stderr, request.json()
            request.raise_for_status()

class DirectoryUploader(GirderBase):
    def __init__(self, girder_token, base_url, job_id):
        self._dir = dir
        self._base_url = base_url
        self._job_id = job_id
        super(DirectoryUploader, self).__init__(girder_token)

    def run(self):
        job_url = '%s/jobs/%s' % ( self._base_url, self._job_id)

        r = requests.get(job_url, headers=self._headers)
        self._check_status(r)

        job = r.json()

        for i in job['output']:

            if 'itemId' in i and 'path' in i:
                item_id = i['itemId']
                path_spec = i['path']
                self._upload(item_id, path_spec)

    def _upload_file(self, name, path, parent_id):
        datalen = os.path.getsize(path)

        params = {
            'parentType': 'item',
            'parentId': parent_id,
            'name': name,
            'size': datalen
        }

        r = requests.post( '%s/file' % self._base_url, params=params, headers=self._headers)
        self._check_status(r)
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

                m = MultipartEncoder(
                  fields=[('uploadId',  upload_id),
                          ('offset', str(uploaded)),
                          ('chunk', (name, part, 'application/octet-stream'))]

                )

                self._headers['Content-Type'] = m.content_type

                r = requests.post('%s/file/chunk' % self._base_url, params=params,
                                         data=m, headers=self._headers)
                self._check_status(r)

                uploaded += chunk_size

    def _upload(self, parent_id, path, pattern=None):
        if os.path.isdir(path):
            for root,_ , file_list in os.walk(path):
                for filename in file_list:
                    file_path = os.path.join(root, filename)

                    name = file_path
                    if not path.startswith('/'):
                        name = os.path.relpath(file_path, os.getcwd())

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
            else: raise

    def _download_item(self, item_id, target_path):

        item_files_url = '%s/item/%s/files' % (self._base_url, item_id)
        r = requests.get(item_files_url, headers=self._headers)
        self._check_status(r)

        files = r.json()

        if len(files) == 1:
            item_url = '%s/item/%s/download' % (self._base_url, item_id)
            r = requests.get(item_url, headers=self._headers)
            self._check_status(r)
            dest_path = os.path.join(self._dest, target_path, files[0]['name'])

            self._mkdir(os.path.dirname(dest_path))
            with open(dest_path, 'w') as fp:
                fp.write(r.content)
        elif len(files) > 1:
            # Download the item in zip format
            item_url = '%s/item/%s/download' % (self._base_url, item_id)
            r = requests.get(item_url, headers=self._headers)
            self._check_status(r)
            files = zipfile.ZipFile(io.BytesIO(r.content))

            dest_path = os.path.join(self._dest, target_path)

            temp_dir = None
            try:
                temp_dir = tempfile.mkdtemp()
                files.extractall(temp_dir)
                item_dir = os.listdir(temp_dir)

                if len(item_dir) != 1:
                    raise Exception('Expecting single item directory, got: %s' % len(item_dir))

                item_dir = os.path.join(temp_dir, item_dir[0])
                if not item_dir.endswith('/'):
                    item_dir += '/'

                for dir, subdirs, files in os.walk(item_dir):
                    for f in files:
                        src = os.path.join(item_dir, dir, f)
                        dest = os.path.join(dest_path, dir.replace(item_dir, ''), f)
                        self._mkdir(os.path.dirname(dest))
                        shutil.copy(src, dest)
            finally:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)

    def run(self):
        job_url = '%s/jobs/%s' % ( self._base_url, self._job_id)

        r = requests.get(job_url, headers=self._headers)
        self._check_status(r)

        job = r.json()

        for i in job['input']:
            item_id = i['itemId']
            target_path = i['path']
            self._download_item(item_id, target_path)


def main():
    parser = argparse.ArgumentParser(description='Girder client for download/upload')

    # Common arguments
    parser.add_argument('--token', help='The Grider token to use when access server', required=True)
    parser.add_argument('--url', help='Base URL for Girder ops', required=True)

    subparsers = parser.add_subparsers(title="actions", dest='action')

    # Upload
    upload_parser = subparsers.add_parser('upload', help='Upload paths to girder items')
    upload_parser.add_argument('--job', help='The job to upload output for', required=True)

    # Download
    download_parser = subparsers.add_parser('download', help='Download file from a Girder item')
    download_parser.add_argument('--job', help='The job to download input for', required=True)
    download_parser.add_argument('--dir', help='The target directory', required=True)

    config = parser.parse_args()

    if config.action == 'upload':
        DirectoryUploader(config.token, config.url, config.job).run()
    elif config.action == 'download':
        JobInputDownloader(config.token, config.url, config.job, config.dir).run()


if __name__ == "__main__":
    main()





