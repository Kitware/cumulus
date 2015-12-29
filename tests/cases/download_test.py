import unittest
import httmock
import mock
import json
from jsonpath_rw import parse

import cumulus
from  cumulus.transport.files.download import download_path

class DownloadTestCase(unittest.TestCase):

    def setUp(self):
        self._update = False
        self._item_requests = []
        self._file_requests = []
        self._folder_requests = []

    def test_import_path(self):
        file = {
                'name': 'test.txt',
                'mode': 1,
                'size': 123
        }

        folder = {
                  'name':  'folder',
                  'mode': 0040000,
                  'size': 1234
        }

        cluster_connection = mock.MagicMock()
        cluster_connection.list.side_effect = [[file, folder], [file]]

        girder_token = 'dummy'
        parent = {
            '_id': 'dummy_id'
        }
        path = '/my/path'
        assetstore_id = 'dummy_id'

        # Mock create item
        def _create_item(url, request):
            content = {
                '_id': 'dummy'
            }
            content = json.dumps(content)
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            self._item_requests.append(request)

            return httmock.response(200, content, headers, request=request)

        item_url = '/api/v1/item'
        create_item = httmock.urlmatch(
            path=r'^%s$' % item_url, method='POST')(_create_item)

        # Mock create file
        def _create_file(url, request):
            content = {
                '_id': 'dummy'
            }
            content = json.dumps(content)
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            self._file_requests.append(request)

            return httmock.response(200, content, headers, request=request)

        file_url = '/api/v1/sftp_assetstores/%s/files' % assetstore_id
        create_file = httmock.urlmatch(
            path=r'^%s$' % file_url, method='POST')(_create_file)

        # Mock create folder
        def _create_folder(url, request):
            content = {
                '_id': 'dummy'
            }
            content = json.dumps(content)
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            self._folder_requests.append(request)

            return httmock.response(200, content, headers, request=request)

        folder_url = '/api/v1/folder'
        create_folder = httmock.urlmatch(
            path=r'^%s$' % folder_url, method='POST')(_create_folder)

        with httmock.HTTMock(create_item, create_file, create_folder):
            download_path(cluster_connection, girder_token, parent, path,
                'sftp_assetstores', assetstore_id, upload=False)


        self.assertEqual(len(self._item_requests), 2)
        self.assertEqual(len(self._file_requests), 2)
        self.assertEqual(len(self._folder_requests), 1)


