import os

from girder_client import GirderClient
import requests

import cumulus
from cumulus.common import check_status


def _upload_file(cluster_connection, girder_client, file, path):
    r = requests.get(
        '%s/file/%s/download' % (girder_client.urlBase, file['_id']),
        headers={'Girder-Token': girder_client.token}, stream=True)
    check_status(r)
    cluster_connection.put(r.raw, os.path.join(path, file['name']))


def _upload_item(cluster_connection, girder_client, item, path):
    offset = 0
    params = {
        'limit': 50,
        'offset': offset
    }

    while True:
        files = girder_client.get('item/%s/files' % item['_id'],
                                  parameters=params)

        for file in files:
            _upload_file(cluster_connection, girder_client, file, path)

        offset += len(files)
        if len(files) < 50:
            break


def _upload_items(cluster_connection, girder_client, folder_id, path):
    for item in girder_client.listItem(folder_id):
        _upload_item(cluster_connection, girder_client, item, path)


def _upload_path(cluster_connection, girder_client, folder_id, path):
    # First process items
    _upload_items(cluster_connection, girder_client, folder_id, path)

    # Now folders
    for folder in girder_client.listFolder(folder_id):
        folder_path = os.path.join(path, folder['name'])
        cluster_connection.mkdir(folder_path)
        _upload_path(cluster_connection, girder_client, folder['_id'],
                     folder_path)


def upload_path(cluster_connection, girder_token, folder_id, path):
    girder_client = GirderClient(apiUrl=cumulus.config.girder.baseUrl)
    girder_client.token = girder_token

    _upload_path(cluster_connection, girder_client, folder_id, path)
