import boto3

class ClientErrorCode:
    AuthFailure  = 'AuthFailure'
    InvalidParameterValue = 'InvalidParameterValue'

def get_ec2_client(profile):
    aws_access_key_id = profile['accessKeyId']
    aws_secret_access_key = profile['secretAccessKey']
    region_name = profile.get('regionName')

    client = boto3.client(
        'ec2', aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name)

    return client
