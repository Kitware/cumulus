from cumulus.ansible.tasks.inventory import AnsibleInventory
import boto3.ec2
from itertools import groupby
import json
import os

from base import Provider
from girder.api.rest import ModelImporter


class EC2Provider(Provider):
    def __init__(self, profile):
        super(EC2Provider, self).__init__(profile)

        if not hasattr(self, "secretAccessKey"):
            profile = ModelImporter.model('aws', 'cumulus').load(
                self.girder_profile_id)

            self.secretAccessKey = profile.get('secreteAccessKey', None)

    def _get_instance_vars(self, instance):
        """
        Determine what to set as host specific variables in the dynamic
        inventory output. instance is a boto.ec2.instance.
        """
        return {
            'instance_id': instance.id,
            'private_ip': instance.private_ip_address,
            'public_ip': instance.public_ip_address,
        }

    def _instances_by_name(self, instances):
        """
        Return a generator of the instances grouped by their
        ec2_pod_instance_name tag.
        """
        return groupby(instances,
                       key=lambda instance:
                       {i['Key']: i['Value']
                        for i in instance.tags}['ec2_pod_instance_name'])

    @staticmethod
    def get_regions():
        if os.environ.get('AWS_REGIONS', '') != '':
            return set(os.environ.get('AWS_REGIONS').split(','))
        else:
            return set(['us-east-1', 'us-west-1', 'us-west-2'])

    def get_inventory(self, cluster_id):
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
        for region in self.get_regions():
            ec2 = boto3.resource(
                'ec2',
                region,
                aws_access_key_id=self.accessKeyId,
                aws_secret_access_key=self.secretAccessKey)

            region_instances = ec2.instances.filter(Filters=[
                {'Name': 'tag:ec2_pod', 'Values': [cluster_id]},
                {'Name': 'instance-state-name', 'Values': ['running']}])

            instances += [i for i in region_instances]

        # Build up main inventory, instance_name is something like "head" or "node"
        # instance_name_instances are the boto.ec2.instance objects that have an
        # ec2_pod_instance_name tag value of instance_name
        for (instance_name, instance_name_instances) \
                in self._instances_by_name(instances):

            inventory[instance_name] = {
                'hosts': [x.public_ip_address
                          for x in instance_name_instances]
            }

        # Build up _meta/hostvars for individual instances
        hostvars = {instance.public_ip_address:
                    self._get_instance_vars(instance)
                    for instance in instances}

        if hostvars:
            inventory['_meta'] = {
                'hostvars': hostvars
            }

        return inventory

    def get_volumes(self):
        pass

Provider.register('ec2', EC2Provider)


if __name__ == "__main__":
    REQUIRED_ENV_VARS = ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                         'CLUSTER_ID',)

    for required_env_var in REQUIRED_ENV_VARS:
        if os.environ.get(required_env_var, '') == '':
            raise Exception('Required env var %s not given to Cumulus'
                            'inventory.' % required_env_var)

    p = Provider({
        "accessKeyId": os.environ.get("AWS_ACCESS_KEY_ID"),
        "secretAccessKey": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "type": "ec2"
    })

    print(json.dumps(p.get_inventory(os.environ.get('CLUSTER_ID'))))
