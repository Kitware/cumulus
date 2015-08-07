import json

from tests import base
from server import _validate_key


def setUpModule():
    base.enabledPlugins.append('ssh')
    base.startServer()


def tearDown():
    base.stopServer()


class SshTestCase(base.TestCase):

    def setUp(self):
        super(SshTestCase, self).setUp()

        self.user = self.model('user').createUser(
            email='not.a.real.email@mail.com',
            login='bobby',
            firstName='first',
            lastName='last',
            password='password',
        )
        self.valid_key = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDJ0wahxwaNbCDdbRll9FypQRXQv5PXQSTh1IeSynTcZZWSyQH4JhoI0lb3/IW7GllIkWblEuyv2SHzXMKRaaFuwmnU1zsY6Y55N6DJt0e9TvieT8MfaM2e7qqaN+0RS2aFb8iw3i+G80tmFVJWuNm7AITVVPf60Nbc5Bgk9qVIa4BakJ3SmW0p/iHT3CStb/k+psevFYyYCEw5l3+3ejPh9b/3423yRzq5r0cyOw8y8fIe4JV8MlE4z2huc/o9Xpw8mzNim7QdobNOylwJsvIYtB4d+MTqvsnt16e22BS/FKuTXx6jGRFFtYNWwwDQe9IIxYb6dPs1XPKVx081nRUwNjar2um41XUOhPx1N5+LfbrYkACVEZiEkW/Ph6hu0PsYQXbL00sWzrzIunixepn5c2dMnDvugvGQA54Z0EXgIYHnetJp2Xck1pJH6oNSSyA+5Mx5QAH5MFNL3YOnGxGBLrkUfK9Ff7QOiZdqXbZoXXS49WtL42Jsv8SgFu3w5NLffvD6/vCOBHwWxh+8VLg5n28M7pZ8+xyMBidkGkG9di2PfV4XsSAeoIc5utgbUJFT6URr2pW9KT4FxTq/easgiJFZUz48SNAjcBneElB9bjAaGf47BPfCNsIAWU2c9MZJWjURpWtzfk21k2/BAfBPs2VNb8dapY6dNinxLqbPIQ== your_email@example.com'

    def tearDown(self):
        #base.stopServer()
        pass

    def test_validate_key(self):
        self.assertFalse(_validate_key('bogus'), 'Key should in invalid')
        self.assertTrue(_validate_key(self.valid_key), 'Key should in valid')

    def test_set_key(self):

        body = json.dumps({
            'publickey': self.valid_key
        })

        resp = self.request('/user/%s/ssh/publickey' % str(self.user['_id']), method='PATCH',
                            type='application/json', body=body, user=self.user)

        self.assertStatusOk(resp)
        self.assertEqual(self.model('user').load(self.user['_id'], force=True)['sshkey'], self.valid_key, 'Updated key doesn\'t match')

        body = json.dumps({
            'publickey': 'bogus'
        })

        resp = self.request('/user/%s/ssh/publickey' % str(self.user['_id']), method='PATCH',
                            type='application/json', body=body, user=self.user)
        self.assertStatus(resp, 400)

    def test_get_key(self):
        body = json.dumps({
            'publickey': self.valid_key
        })

        resp = self.request('/user/%s/ssh/publickey' % str(self.user['_id']), method='PATCH',
                            type='application/json', body=body, user=self.user)
        self.assertStatusOk(resp)

        resp = self.request('/user/%s/ssh/publickey' % str(self.user['_id']), method='GET',
                            user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['publickey'], self.valid_key, 'Fetch key doesn\'t match')

    def test_set_passphrase(self):
        passphrase = 'openup'
        body = json.dumps({
            'passphrase': passphrase
        })

        resp = self.request('/user/%s/ssh/passphrase' % str(self.user['_id']), method='PATCH',
                            type='application/json', body=body, user=self.user)

        self.assertStatusOk(resp)
        self.assertEqual(self.model('user').load(self.user['_id'], fields=['passphrase'],
                                                 force=True)['passphrase'],
                         passphrase, 'Updated passphrase doesn\'t match')

        resp = self.request('/user/%s/ssh/passphrase' % str(self.user['_id']), method='PATCH',
                            type='application/json', body=json.dumps({}), user=self.user)
        self.assertStatus(resp, 400)

    def test_get_passphrase(self):
        passphrase = 'openup'
        body = json.dumps({
            'passphrase': passphrase
        })
        resp = self.request('/user/%s/ssh/passphrase' % str(self.user['_id']), method='PATCH',
                            type='application/json', body=body, user=self.user)
        self.assertStatusOk(resp)

        resp = self.request('/user/%s/ssh/passphrase' % str(self.user['_id']), method='GET',
                            user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['passphrase'], passphrase, 'Fetch passphrase doesn\'t match')

