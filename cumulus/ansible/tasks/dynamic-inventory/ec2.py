#!/usr/bin/env python
import boto.ec2
from itertools import groupby
import json
import os

"""
Retrieve dynamic inventory of a specific Cumulus cluster.
See: http://docs.ansible.com/ansible/intro_dynamic_inventory.html
     http://docs.ansible.com/ansible/developing_inventory.html

To properly implement an Ansible dynamic inventory, this script should take
--list for all hosts, or take an individual host and return just its inventory
data. This currently implements --list only.

Arguments need to be passed through environment variables since Ansible
doesn't allow passing arguments through the -i value.

Sample output:
{
    "master": {
        "hosts": [
            "52.37.147.107"
        ]
    },
    "exec": {
        "hosts": [
            "52.24.45.174"
        ]
    },
    "_meta": {
        "hostvars": {
            "52.24.45.174": {
                "private_ip_address": "172.31.38.31"
            },
            "52.37.147.107": {
                "private_ip_address": "172.31.41.5"
            }
        }
    }
}
"""


def instance_filter(instance):
    """
    What determines if an instance should be returned for the inventory, where
    instance is a boto.ec2.instance.
    """
    return (instance.state == 'running' and
            'ec2_pod' in instance.tags and
            'ec2_pod_instance_name' in instance.tags and
            instance.tags['ec2_pod'] == os.environ.get('CLUSTER_ID'))


def get_instance_vars(instance):
    """
    Determine what to set as host specific variables in the dynamic
    inventory output. instance is a boto.ec2.instance.
    """
    return {
        'private_ip': instance.private_ip_address,
        'ansible_ssh_private_key_file': os.environ.get('PRIVATE_KEY_FILE', '')
    }


def instances_by_name(instances):
    """
    Return a generator of the instances grouped by their ec2_pod_instance_name
    tag.
    """
    return groupby(instances,
                   key=lambda instance: instance.tags['ec2_pod_instance_name'])


def get_inventory():
    """
    Retrieve the inventory from a set of regions in an Ansible Dynamic
    Inventory compliant format (see
    http://docs.ansible.com/ansible/developing_inventory.html#script-conventions).

    Instances are filtered through instance_filter, grouped by the
    ec2_pod_instance_name tag, and contain host specific variables
    according to get_instance_vars.
    """
    inventory = {}
    instances = []

    # Gather all instances that pass instance_filter into instances
    for region in get_regions():
        ec2 = boto.ec2.connect_to_region(
            region,
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))

        region_instances = sum([x.instances for x in
                                ec2.get_all_reservations()], [])
        instances += [x for x in region_instances if instance_filter(x)]

    # Build up main inventory, instance_name is something like "head" or "node"
    # instance_name_instances are the boto.ec2.instance objects that have an
    # ec2_pod_instance_name tag value of instance_name
    for (instance_name, instance_name_instances) in instances_by_name(
            instances):
        inventory[instance_name] = {
            'hosts': [x.ip_address for x in instance_name_instances]
        }

    # Build up _meta/hostvars for individual instances
    hostvars = {instance.ip_address: get_instance_vars(instance)
                for instance in instances}

    if hostvars:
        inventory['_meta'] = {
            'hostvars': hostvars
        }

    return inventory


def get_regions():
    if os.environ.get('AWS_REGIONS', '') != '':
        return set(os.environ.get('AWS_REGIONS').split(','))
    else:
        return set(['us-east-1', 'us-west-1', 'us-west-2'])


if __name__ == '__main__':
    REQUIRED_ENV_VARS = ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                         'CLUSTER_ID',)

    for required_env_var in REQUIRED_ENV_VARS:
        if os.environ.get(required_env_var, '') == '':
            raise Exception('Required env var %s not given to Cumulus'
                            'inventory.' % required_env_var)

    print(json.dumps(get_inventory()))
