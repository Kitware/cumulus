import random
import string
import os
from paramiko.rsakey import RSAKey
import requests

import cumulus
from cumulus.starcluster.tasks.celery import command
from cumulus.starcluster.tasks.common import _check_status


@command.task
def generate_key_pair(user, girder_token=None):
    '''
    Task to generate a new key pair for a user.
    '''
    new_key = RSAKey.generate(bits=4096)
    passphrase = ''.join(random.SystemRandom()
                         .choice(string.ascii_uppercase +
                                 string.digits) for _ in range(64))
    key_path = os.path.join(cumulus.config.ssh.keyStore, user['_id'])

    new_key.write_private_key_file(key_path, passphrase)

    # Update passphrase and public key on user model
    patch_url = '%s/user/%s/ssh/passphrase' % (cumulus.config.girder.baseUrl,
                                               user['_id'])
    headers = {'Girder-Token':  girder_token}

    body = {
        'passphrase': passphrase
    }

    request = requests.patch(patch_url, json=body, headers=headers)
    _check_status(request)

    comment = 'cumulus generated access key'
    public_key = '%s %s %s' % (new_key.get_name(), new_key.get_base64(),
                               comment)

    patch_url = '%s/user/%s/ssh/publickey' % (cumulus.config.girder.baseUrl,
                                              user['_id'])

    body = {
        'publickey': public_key
    }
    request = requests.patch(patch_url, json=body, headers=headers)
    _check_status(request)
