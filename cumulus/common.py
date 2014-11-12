import requests
import cumulus

def _authenticate(base_url, username, password):
    r = requests.get('%s/user/authentication' % base_url,
                                auth=(username, password))
    r.raise_for_status()

    return r.json()['authToken']['token']

_token = None

def girder_token():
    if not _token:
        _token = _authenticate(cumulus.config.girder.baseUrl,
                               cumulus.config.girder.user,
                               cumulus.config.girder.password)

    return _token
