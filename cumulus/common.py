def get_config_url(base_url, config_id):
    return '%s/starcluster-configs/%s?format=ini' \
           % (base_url, config_id)
