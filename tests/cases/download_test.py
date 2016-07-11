import unittest
import httmock
import mock
import json
from jsonpath_rw import parse
import stat

import cumulus
from cumulus.transport.files.download import download_path
from cumulus.transport.files.download import _ensure_path

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
                  'mode': stat.S_IFDIR,
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
            content = json.dumps(content).encode('utf8')
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
            content = json.dumps(content).encode('utf8')
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
            content = json.dumps(content).encode('utf8')
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            self._folder_requests.append(request)

            return httmock.response(200, content, headers, request=request)

        folder_url = '/api/v1/folder'
        create_folder = httmock.urlmatch(
            path=r'^%s$' % folder_url, method='POST')(_create_folder)

        # Mock list folder
        def _list_folder(url, request):
            content = []
            content = json.dumps(content).encode('utf8')
            headers = {
                'content-length': len(content),
                'content-type': 'application/json'
            }

            return httmock.response(200, content, headers, request=request)

        folder_url = '/api/v1/folder'
        list_folder = httmock.urlmatch(
            path=r'^%s$' % folder_url, method='GET')(_list_folder)

        with httmock.HTTMock(create_item, create_file, create_folder, list_folder):
            download_path(cluster_connection, girder_token, parent, path,
                'sftp_assetstores', assetstore_id, upload=False)


        self.assertEqual(len(self._item_requests), 2)
        self.assertEqual(len(self._file_requests), 2)
        self.assertEqual(len(self._folder_requests), 1)

    def test_ensure_path(self):
        girder_client = mock.MagicMock()

        # First test with path already in girder_folders
        folders = []
        for i in range(0, 3):
            folders.append({
                '_id': 'folderId%d' % i
            })
        girder_client.createFolder.side_effect = folders
        girder_client.listFolder.return_value = iter([])
        girder_folders = {
            'a/b': 'id'
        }
        parent = 'dummy'
        path = 'a/b/c/d/e'
        folder_id = _ensure_path(girder_client, girder_folders, parent, path)

        expect_girder_folders = {
            'a/b/c': 'folderId0',
            'a/b/c/d': 'folderId1',
            'a/b/c/d/e': 'folderId2',
            'a/b': 'id'
        }
        self.assertEqual(girder_folders, expect_girder_folders)
        self.assertEqual(folder_id, 'folderId2')

        # Test with empty girder_folders ( all paths need to be created )
        folders = []
        for i in range(0, 5):
            folders.append({
                '_id': 'folderId%d' % i
            })
        girder_client.createFolder.side_effect = folders
        girder_client.listFolder.return_value = iter([])
        girder_folders = {}
        parent = 'dummy'
        path = 'a/b/c/d/e'
        folder_id = _ensure_path(girder_client, girder_folders, parent, path)

        expect_girder_folders = {
            'a': 'folderId0',
            'a/b': 'folderId1',
            'a/b/c': 'folderId2',
            'a/b/c/d': 'folderId3',
            'a/b/c/d/e': 'folderId4'
        }
        self.assertEqual(girder_folders, expect_girder_folders)
        self.assertEqual(folder_id, 'folderId4')

        # Test with folders already in Girder
        folders = []
        for i in range(0, 3):
            folders.append({
                '_id': 'folderId%d' % i
            })
        girder_client.createFolder.side_effect = folders
        girder_client.listFolder.side_effect = [
            iter([{
                '_id': 'girderFolder0'
            }]),
            iter([{
                '_id': 'girderFolder1'
            }]),
            iter([])
        ]
        girder_folders = {}
        parent = 'dummy'
        path = 'a/b/c/d/e'
        folder_id = _ensure_path(girder_client, girder_folders, parent, path)

        expect_girder_folders = {
            'a': 'girderFolder0',
            'a/b': 'girderFolder1',
            'a/b/c': 'folderId0',
            'a/b/c/d': 'folderId1',
            'a/b/c/d/e': 'folderId2'
        }
        self.assertEqual(girder_folders, expect_girder_folders)
        self.assertEqual(folder_id, 'folderId2')

        # Test with folders already in Girder and already in girder_folders
        folders = []
        for i in range(0, 3):
            folders.append({
                '_id': 'folderId%d' % i
            })
        girder_client.createFolder.side_effect = folders
        girder_client.listFolder.side_effect = iter([
            iter([{
                '_id': 'girderFolder0'
            }]),
            iter([])
        ])
        girder_folders = {
            'a': 'dummy'
        }
        parent = 'dummy'
        path = 'a/b/c/d/e'
        folder_id = _ensure_path(girder_client, girder_folders, parent, path)

        expect_girder_folders = {
            'a': 'dummy',
            'a/b': 'girderFolder0',
            'a/b/c': 'folderId0',
            'a/b/c/d': 'folderId1',
            'a/b/c/d/e': 'folderId2'
        }
        self.assertEqual(girder_folders, expect_girder_folders)
        self.assertEqual(folder_id, 'folderId2')
