from tests import base
import json


def setUpModule():
    base.enabledPlugins.append('cumulus')
    base.startServer()


def tearDownModule():
    base.stopServer()


class ScriptTestCase(base.TestCase):

    def setUp(self):
        super(ScriptTestCase, self).setUp()

        users = ({
            'email': 'cumulus@email.com',
            'login': 'cumulus',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        }, {
            'email': 'regularuser@email.com',
            'login': 'regularuser',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        })
        self._cumulus, self._user = \
            [self.model('user').createUser(**user) for user in users]

        self._group = self.model('group').createGroup('cumulus', self._cumulus)

    def test_create(self):
        body = {
            'commands': ['echo "test"'],
            'name': 'test'
        }
        body = json.dumps(body)

        r = self.request('/scripts', method='POST',
                         type='application/json', body=body, user=self._cumulus)
        self.assertStatus(r, 201)

        self.assertEqual(r.json['commands'], ['echo "test"'])
        self.assertEqual(r.json['name'], 'test')

        body = {
            'commands': ['echo "test"'],
        }
        body = json.dumps(body)

        r = self.request('/scripts', method='POST',
                         type='application/json', body=body, user=self._cumulus)
        self.assertStatus(r, 400)

    def test_import(self):
        body = {
            'name': 'test'
        }
        body = json.dumps(body)

        r = self.request('/scripts', method='POST',
                         type='application/json', body=body, user=self._cumulus)
        self.assertStatus(r, 201)

        self.assertEqual(r.json['name'], 'test')
        script_id = r.json['_id']

        r = self.request('/scripts/546a1844ff34c70456111185/import', method='PATCH',
                         type='text/plain', body='echo "test1"\necho "test2"', user=self._cumulus)
        self.assertStatus(r, 404)

        r = self.request('/scripts/%s/import' % str(script_id), method='PATCH',
                         type='text/plain', body='echo "test1"\necho "test2"', user=self._cumulus)
        self.assertStatusOk(r)
        expected_script = {
            'commands': [
                'echo "test1"',
                'echo "test2"'
            ],
            'name': 'test'
        }
        del r.json['_id']
        self.assertEqual(r.json, expected_script)



    def test_get(self):
        body = {
            'commands': ['echo "test"'],
            'name': 'test'
        }
        body = json.dumps(body)

        r = self.request('/scripts', method='POST',
                         type='application/json', body=body, user=self._cumulus)
        self.assertStatus(r, 201)

        self.assertEqual(r.json['commands'], ['echo "test"'])
        self.assertEqual(r.json['name'], 'test')
        script_id = r.json['_id']

        r = self.request('/scripts/546a1844ff34c70456111185', method='GET',
                         user=self._cumulus)
        self.assertStatus(r, 404)

        r = self.request('/scripts/%s' % str(script_id), method='GET',
                         user=self._cumulus)
        self.assertStatusOk(r)
        expected_script = {
            'commands': [
                'echo "test"'
            ],
            'name': 'test'
        }
        del r.json['_id']
        self.assertEquals(r.json, expected_script)


    def test_delete(self):
        body = {
            'commands': ['echo "test"'],
            'name': 'test'
        }
        body = json.dumps(body)

        r = self.request('/scripts', method='POST',
                         type='application/json', body=body, user=self._cumulus)
        self.assertStatus(r, 201)

        self.assertEqual(r.json['commands'], ['echo "test"'])
        self.assertEqual(r.json['name'], 'test')
        script_id = r.json['_id']

        r = self.request('/scripts/%s' %
                         str(script_id), method='DELETE', user=self._user)
        self.assertStatus(r, 403)

        r = self.request('/scripts/%s' %
                         str(script_id), method='DELETE', user=self._cumulus)
        self.assertStatusOk(r)

        r = self.request('/scripts/%s' %
                         str(script_id), method='GET', user=self._cumulus)
        self.assertStatus(r, 404)
