import starcluster.config
import starcluster.logger
import starcluster.exception
from starcluster.awsutils import EasyEC2
from starcluster.sshutils import SSHClient
import os
import traceback
from jsonpath_rw import parse
from contextlib import contextmanager

import cumulus
from cumulus.constants import ClusterType
from cumulus.common import create_config_request


def _log_exception(ex):
    log = starcluster.logger.get_starcluster_logger()
    log.error(traceback.format_exc())

@contextmanager
def get_ssh_connection(girder_token, cluster):
    conn = None
    if cluster['type'] == ClusterType.TRADITIONAL:
        try:
            username = parse('config.ssh.user').find(cluster)[0].value
            hostname = parse('config.host').find(cluster)[0].value
            passphrase = parse('config.ssh.passphrase').find(cluster)[0].value

            key_path = os.path.join(cumulus.config.ssh.keyStore, cluster['_id'])

            conn = SSHClient(host=hostname, username=username, private_key=key_path,
                             private_key_pass=passphrase, timeout=5)

            conn.connect()

            yield conn
        finally:
            if conn:
                conn.close()
    else:
        cluster_id = cluster['_id']
        config_id = cluster['config']['_id']
        config_request = create_config_request(girder_token,
                                               cumulus.config.girder.baseUrl,
                                               config_id)
        config = starcluster.config.StarClusterConfig(config_request)

        config.load()
        cm = config.get_cluster_manager()
        sc = cm.get_cluster(cluster_id)
        master = sc.master_node
        master.user = sc.cluster_user
        conn = master.ssh

        yield conn


def get_easy_ec2(profile):
    aws_access_key_id = profile['accessKeyId']
    aws_secret_access_key = profile['secretAccessKey']
    aws_region_name = profile['regionName']
    aws_region_host = profile['regionHost']
    ec2 = EasyEC2(aws_access_key_id, aws_secret_access_key,
                  aws_region_name=aws_region_name,
                  aws_region_host=aws_region_host)

    return ec2
