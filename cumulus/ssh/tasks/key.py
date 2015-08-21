import random
import string
import os
from paramiko.rsakey import RSAKey
import requests
import starcluster.logger

import cumulus
from cumulus.starcluster.tasks.celery import command
from cumulus.starcluster.tasks.common import _check_status


@command.task
def generate_key_pair(cluster, girder_token=None):
    '''
    Task to generate a new key pair for a user.
    '''
    cluster_id = cluster['_id']
    status_url = '%s/clusters/%s' \
        % (cumulus.config.girder.baseUrl, cluster_id)
    log = starcluster.logger.get_starcluster_logger()

    try:
        new_key = RSAKey.generate(bits=4096)
        passphrase = ''.join(random.SystemRandom()
                             .choice(string.ascii_uppercase +
                                     string.digits) for _ in range(64))
        key_path = os.path.join(cumulus.config.ssh.keyStore, cluster_id)

        new_key.write_private_key_file(key_path, password=passphrase)

        comment = 'cumulus generated access key'
        public_key = '%s %s %s' % (new_key.get_name(), new_key.get_base64(),
                                   comment)

        # Update passphrase and public key on cluster model
        config_update = {
            'config': {
                'ssh': {
                    'passphrase': passphrase,
                    'publicKey': public_key
                }
            }
        }

        patch_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl,
                                        cluster_id)
        headers = {'Girder-Token':  girder_token}
        request = requests.patch(patch_url, json=config_update, headers=headers)
        _check_status(request)
    except Exception as ex:
        r = requests.patch(status_url, headers=headers,
                           json={'status': 'error'})
        _check_status(r)
        # Log the error message
        log.error(ex.message)
