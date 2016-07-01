from cumulus.ansible.tasks.providers import CloudProvider


class ClientErrorCode:
    AuthFailure = 'AuthFailure'
    InvalidParameterValue = 'InvalidParameterValue'


def get_ec2_client(profile):
    return CloudProvider(profile)
