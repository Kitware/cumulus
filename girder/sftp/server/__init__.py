from girder import events
from girder.api import access
from girder.api.v1.assetstore import Assetstore
from girder.constants import AssetstoreType
from girder.utility.model_importer import ModelImporter

from .assetstore import SftpAssetstoreAdapter
from .credentials import retrieve_credentials
from .rest import SftpAssetstoreResource

def getAssetstore(event):
    assetstore = event.info
    if assetstore['type'] == AssetstoreType.SFTP:
        event.stopPropagation()
        event.addResponse(SftpAssetstoreAdapter)


def updateAssetstore(event):
    params = event.info['params']
    assetstore = event.info['assetstore']

    if assetstore['type'] == AssetstoreType.SFTP:
        assetstore[AssetstoreType.SFTP] = {
            'host': params.get('host', assetstore['sftp']['host']),
            'user': params.get('host', assetstore['sftp']['user']),
            'keystore': params.get('host', assetstore['sftp']['keystore'])
        }

def load(info):

    AssetstoreType.SFTP = 'sftp'
    events.bind('assetstore.adapter.get', 'sftp', getAssetstore)
    events.bind('assetstore.update', 'sftp', updateAssetstore)
    events.bind('assetstore.sftp.credentials.get', 'sftp', retrieve_credentials)

    info['apiRoot'].sftp_assetstores = SftpAssetstoreResource()
