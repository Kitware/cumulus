import os
from jsonpath_rw import parse

from girder.utility.model_importer import ModelImporter
from girder.constants import AccessType
from girder.api.rest import getCurrentUser

import cumulus

def retrieve_credentials(event):
    cluster_id = event.info['authKey']
    user = event.info['user']
    model = ModelImporter.model('cluster', 'cumulus')
    cluster = model.load(cluster_id, user=getCurrentUser(),
                         level=AccessType.READ)

    username = parse('config.ssh.user').find(cluster)[0].value
    key_path = os.path.join(cumulus.config.ssh.keyStore, cluster_id)
    passphrase = parse('config.ssh.passphrase').find(cluster)[0].value

    if user != username:
        raise Exception('User doesn\'t match cluster user id ')

    event.stopPropagation()
    event.addResponse((key_path, passphrase))
