import unittest
import httmock
import os
import json

import cumulus
from cumulus.ssh.tasks import key

class KeyTestCase(unittest.TestCase):

    def setUp(self):
        self._set_passphrase = False
        self._set_publickey = False

    def test_key_generate(self):
        user = {
            '_id': '55c3a698f6571011a48f6817'
        }

        key_path = os.path.join(cumulus.config.ssh.keyStore, user['_id'])
        try:
            os.remove(key_path)
        except OSError:
            pass

        def _set_passphrase(url, request):
            self._set_passphrase = 'passphrase' in json.loads(request.body)
            return httmock.response(200, None, {}, request=request)


        passphrase_url = '/api/v1/user/%s/ssh/passphrase' % user['_id']
        set_passphrase = httmock.urlmatch(
            path=r'^%s$' % passphrase_url, method='PATCH')(_set_passphrase)

        def _set_publickey(url, request):
            self._set_publickey = 'publickey' in json.loads(request.body)
            print json.loads(request.body)

            return httmock.response(200, None, {}, request=request)


        publickey_url = '/api/v1/user/%s/ssh/publickey' % user['_id']
        set_publickey = httmock.urlmatch(
            path=r'^%s$' % publickey_url, method='PATCH')(_set_publickey)

        with httmock.HTTMock(set_passphrase, set_publickey):
            key.generate_key_pair(user)

        self.assertTrue(os.path.exists(key_path), 'Key was not created')
        self.assertTrue(self._set_passphrase, 'Update passphrase was not called')
        self.assertTrue(self._set_publickey, 'Update public key was not called')

