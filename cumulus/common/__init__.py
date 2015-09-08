from __future__ import absolute_import
import urllib2
from starcluster.awsutils import EasyEC2


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


def get_easy_ec2(profile):
    aws_access_key_id = profile['accessKeyId']
    aws_secret_access_key = profile['secretAccessKey']
    aws_region_name = profile['regionName']
    aws_region_host = profile['regionHost']
    ec2 = EasyEC2(aws_access_key_id, aws_secret_access_key,
                  aws_region_name=aws_region_name,
                  aws_region_host=aws_region_host)

    return ec2
