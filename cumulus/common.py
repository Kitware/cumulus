import urllib2


def get_config_url(base_url, config_id):
    return '%s/starcluster-configs/%s?format=ini' \
           % (base_url, config_id)


def create_config_request(girder_token, base_url, config_id):
    config_url = get_config_url(base_url, config_id)

    headers = {
        'Girder-Token': girder_token
    }

    config_request = urllib2.Request(config_url, headers=headers)

    return config_request
