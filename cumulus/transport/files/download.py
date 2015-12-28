import os
import stat

from girder_client import GirderClient

import cumulus

def _import_path(cluster_connection, girder_client, parent, path,
                 assetstore_url, assetstore_id, upload=False,
                 parent_type='folder'):

    for p in cluster_connection.list(path):
        print p
        name  = p['name']

        full_path = os.path.join(path, name)
        if name in ['.', '..']:
            continue
        if stat.S_ISDIR(p['mode']):

            folder = girder_client.createFolder(parent, name, parentType=parent_type)
            _import_path(cluster_connection, girder_client, folder, full_path, assetstore_url,
                         assetstore_id, upload=upload,
                         parent_type='folder')
        else:
            size = p['size']
            item = girder_client.createItem(parent, name, '')

            if not upload:

                url = '%s/%s/files' % (assetstore_url, assetstore_id)
                body = {
                    'name': name,
                    'itemId': item['_id'],
                    'size': size,
                    'path': full_path
                }
                girder_client.post(url, data=body)
            else:
                with girder_client.get(path) as stream:
                    girder_client.uploadFile(item['_id'], stream, name, size,
                                             parentType='item')


def download_path(cluster_connection, girder_token, parent, path, assetstore_url,
                assetstore_id, upload=False):
    girder_client = GirderClient(apiUrl=cumulus.config.girder.baseUrl)
    girder_client.token = girder_token

    _import_path(cluster_connection, girder_client, parent, path,
                 assetstore_url, assetstore_id, upload=upload)
