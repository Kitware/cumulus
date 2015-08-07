from tests import base
from server import _validate_key


def setUpModule():
    base.enabledPlugins.append('sshkey')
    base.startServer()


def tearDown():
    base.stopServer()


class SshKeyTestCase(base.TestCase):

    def setUp(self):
        super(SshKeyTestCase, self).setUp()

        self.user = self.model('user').createUser(
            email='not.a.real.email@mail.com',
            login='bobby',
            firstName='first',
            lastName='last',
            password='password',
        )
        self.valid_key = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDJ0wahxwaNbCDdbRll9FypQRXQv5PXQSTh1IeSynTcZZWSyQH4JhoI0lb3/IW7GllIkWblEuyv2SHzXMKRaaFuwmnU1zsY6Y55N6DJt0e9TvieT8MfaM2e7qqaN+0RS2aFb8iw3i+G80tmFVJWuNm7AITVVPf60Nbc5Bgk9qVIa4BakJ3SmW0p/iHT3CStb/k+psevFYyYCEw5l3+3ejPh9b/3423yRzq5r0cyOw8y8fIe4JV8MlE4z2huc/o9Xpw8mzNim7QdobNOylwJsvIYtB4d+MTqvsnt16e22BS/FKuTXx6jGRFFtYNWwwDQe9IIxYb6dPs1XPKVx081nRUwNjar2um41XUOhPx1N5+LfbrYkACVEZiEkW/Ph6hu0PsYQXbL00sWzrzIunixepn5c2dMnDvugvGQA54Z0EXgIYHnetJp2Xck1pJH6oNSSyA+5Mx5QAH5MFNL3YOnGxGBLrkUfK9Ff7QOiZdqXbZoXXS49WtL42Jsv8SgFu3w5NLffvD6/vCOBHwWxh+8VLg5n28M7pZ8+xyMBidkGkG9di2PfV4XsSAeoIc5utgbUJFT6URr2pW9KT4FxTq/easgiJFZUz48SNAjcBneElB9bjAaGf47BPfCNsIAWU2c9MZJWjURpWtzfk21k2/BAfBPs2VNb8dapY6dNinxLqbPIQ== your_email@example.com'

    def tearDown(self):
        base.stopServer()

    def test_validate_key(self):
        self.assertFalse(_validate_key('bogus'), 'Key should in invalid')
        self.assertTrue(_validate_key(self.valid_key), 'Key should in valid')

    def test_set_key(self):
        resp = self.request('/user/%s/sshkey' % str(self.user['_id']), method='PATCH',
                            type='text/plain', body=self.valid_key, user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(self.model('user').load(self.user['_id'], force=True)['sshkey'], self.valid_key, 'Updated key doesn\'t match')

        resp = self.request('/user/%s/sshkey' % str(self.user['_id']), method='PATCH',
                            type='text/plain', body='bogon', user=self.user)
        self.assertStatus(resp, 400)

    def test_get_key(self):
        resp = self.request('/user/%s/sshkey' % str(self.user['_id']), method='PATCH',
                            type='text/plain', body=self.valid_key, user=self.user)
        self.assertStatusOk(resp)

        resp = self.request('/user/%s/sshkey' % str(self.user['_id']), method='GET',
                            user=self.user)
        self.assertStatusOk(resp)
        self.assertEqual(resp.json['key'], self.valid_key, 'Fetch key doesn\'t match')

