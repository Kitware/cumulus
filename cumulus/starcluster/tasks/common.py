import starcluster.config
import starcluster.logger
import starcluster.exception
from starcluster.sshutils import SSHClient
import requests
import tempfile
import os
import sys
import traceback
from jsonpath_rw import parse


import cumulus
from cumulus.constants import ClusterType


def _check_status(request):
    if request.status_code != 200:
        print >> sys.stderr, request.content
        request.raise_for_status()


def _write_config_file(girder_token, config_url):
    headers = {'Girder-Token':  girder_token}

    r = requests.get(config_url, headers=headers, params={'format': 'ini'})
    _check_status(r)

    # Write config to temp file
    (fd, config_filepath) = tempfile.mkstemp()

    try:
        os.write(fd, r.text)
    finally:
        os.close(fd)

    return config_filepath


def _log_exception(ex):
    log = starcluster.logger.get_starcluster_logger()
    log.error(traceback.format_exc())

def get_ssh_connection(girder_token, cluster):
    conn = None
    if cluster['type'] == ClusterType.TRADITIONAL:
        username = parse('config.ssh.user').find(cluster)[0].value
        hostname = parse('config.host').find(cluster)[0].value
        passphrase = parse('config.ssh.passphrase').find(cluster)[0].value

        key_path = os.path.join(cumulus.config.ssh.keyStore, cluster['_id'])

        conn = SSHClient(host=hostname, username=username, private_key=key_path,
                         private_key_pass=passphrase, timeout=5)

        conn.connect()

    else:
        config_filepath = None
        try:
            name = cluster['name']
            config_id = cluster['config']['_id']
            config_url = '%s/starcluster-configs/%s?format=ini' \
                % (cumulus.config.girder.baseUrl, config_id)
            config_filepath = _write_config_file(girder_token, config_url)
            config = starcluster.config.StarClusterConfig(config_filepath)

            config.load()
            cm = config.get_cluster_manager()
            sc = cm.get_cluster(name)
            master = sc.master_node
            master.user = sc.cluster_user
            conn = master.ssh
        finally:
            if config_filepath and os.path.exists(config_filepath):
                os.remove(config_filepath)

    return conn
