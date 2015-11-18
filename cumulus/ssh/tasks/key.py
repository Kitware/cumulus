import random
import string
import os
from paramiko.rsakey import RSAKey
import requests
import starcluster.logger

import cumulus
from cumulus.celery import command
from cumulus.common import check_status


def _key_path(profile):
    return os.path.join(cumulus.config.ssh.keyStore, str(profile['_id']))


@command.task
def generate_key_pair(cluster, girder_token=None):
    '''
    Task to generate a new key pair for a user.
    '''
    cluster_id = cluster['_id']
    status_url = '%s/clusters/%s' \
        % (cumulus.config.girder.baseUrl, cluster_id)
    log = starcluster.logger.get_starcluster_logger()
    headers = {'Girder-Token':  girder_token}

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
            },
            'status': 'created'
        }

        patch_url = '%s/clusters/%s' % (cumulus.config.girder.baseUrl,
                                        cluster_id)
        request = requests.patch(patch_url, json=config_update, headers=headers)
        check_status(request)
    except Exception as ex:
        r = requests.patch(status_url, headers=headers,
                           json={'status': 'error'})
        check_status(r)
        # Log the error message
        log.error(ex.message)


@command.task
def delete_key_pair(aws_profile, girder_token):
    path = _key_path(aws_profile)

    if os.path.exists(path):
        os.remove(path)
