import unittest
import httmock
import os
import json
from jsonpath_rw import parse

import cumulus
from cumulus.ssh.tasks import key

class KeyTestCase(unittest.TestCase):

    def setUp(self):
        self._update = False

    def test_key_generate(self):
        cluster = {
            '_id': '55c3a698f6571011a48f6817'
        }

        key_path = os.path.join(cumulus.config.ssh.keyStore, cluster['_id'])
        try:
            os.remove(key_path)
        except OSError:
            pass

        def _update(url, request):
            request_body = json.loads(request.body)
            passphrase = parse('config.ssh.passphrase').find(request_body)
            public_key = parse('config.ssh.publicKey').find(request_body)

            self._update = passphrase and public_key
            return httmock.response(200, None, {}, request=request)

        update_url = '/api/v1/clusters/%s' % cluster['_id']
        update = httmock.urlmatch(
            path=r'^%s$' % update_url, method='PATCH')(_update)

        with httmock.HTTMock(update):
            key.generate_key_pair(cluster)

        self.assertTrue(os.path.exists(key_path), 'Key was not created')
        os.remove(key_path)
        self.assertTrue(self._update, 'Update was not called')


