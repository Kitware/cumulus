import requests
from requests_toolbelt import MultipartEncoder
import os
import json
import argparse
import sys

max_chunk_size = 1024 * 1024 * 64

class DirectoryUploader():
    def __init__(self, dir, base_url, target_collection_id, folder, girder_token):
        self._dir = dir
        self._base_url = base_url
        self._target_collection_id = target_collection_id
        self._root_folder = folder
        self._headers = {'Girder-Token': girder_token}
        self._girder_token = girder_token

    def run(self):

        folder_id = self._create_folder(self._root_folder, self._target_collection_id, 'collection')
        self._upload(folder_id, self._dir)

    def _check_status(self, request):
        if request.status_code != 200:
            print >> sys.stderr, request.json()
            request.raise_for_status()

    def _create_folder(self, name, parent_id, parent_type='folder'):
        params = {'parentType': parent_type, 'parentId': parent_id, 'name': name}

        r = requests.post( '%s/folder' % self._base_url, params=params, headers=self._headers)
        self._check_status(r)
        obj = r.json()

        if '_id' in obj:
            folder_id = obj['_id']
        else:
            raise Exception('Unexpected response: ' + json.dumps(obj))

        return folder_id

    def _upload_file(self, path, parent_id):
        name = os.path.basename(path)

        # TODO will need to stream for large files ...
        with open(path, 'rb') as fp:
            data = fp.read()

        datalen = len(data)
        params = {
            'parentType': 'folder',
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

        while (uploaded != datalen):
            chunk_size = datalen - uploaded
            if chunk_size > max_chunk_size:
                chunk_size = max_chunk_size

            part = data[uploaded:uploaded+chunk_size]

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

    def _upload(self, parent_id, path):
        for root, subdirs, file_list in os.walk(path):
            dir_name = os.path.basename(root)

            parent_id = self._create_folder(dir_name, parent_id)
            for filename in file_list:
                self._upload_file(os.path.join(root, filename), parent_id)

def main():
    parser = argparse.ArgumentParser(description='Girder client for upload')
    # TODO When we do the download we will probably want this
    #subparsers = parser.add_subparsers(title="actions")

    parser.add_argument('--dir', help='Directory to upload', required=True)
    parser.add_argument('--url', help='Base URL for Girder ops', required=True)
    parser.add_argument('--collection', help='Root collection id', required=True)
    parser.add_argument('--folder', help='Root folder name', required=True)
    parser.add_argument('--token', help='The Grider token to use when access server', required=True)

    config = parser.parse_args()

    DirectoryUploader(config.dir, config.url, config.collection, config.folder,
                      config.token).run()

if __name__ == "__main__":
    main()





