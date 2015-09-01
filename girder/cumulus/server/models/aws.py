from starcluster.awsutils import EasyEC2
from starcluster.exception import RegionDoesNotExist, ZoneDoesNotExist
from boto.exception import EC2ResponseError

from girder.constants import AccessType

from .base import BaseModel
from girder.models.model_base import ValidationException
from girder.api.rest import getCurrentUser


class Aws(BaseModel):
    def __init__(self):
        super(Aws, self).__init__()

    def initialize(self):
        self.name = 'aws'
        self.ensureIndices(['userId', 'name'])
        self.exposeFields(level=AccessType.READ, fields=(
            '_id', 'name', 'accessKeyId', 'regionName', 'regionHost',
            'availabilityZone'))

    def validate(self, doc):
        name = doc['name']

        if not name:
            raise ValidationException('A name must be provided', 'name')

        # Check for duplicate names
        query = {
            'name': name,
            'userId': doc['userId']
        }
        if '_id' in doc:
            query['_id'] = {'$ne': doc['_id']}

        if self.findOne(query):
            raise ValidationException('A profile with that name already exists',
                                      'name')

        ec2 = EasyEC2(doc['accessKeyId'],
                      doc['secretAccessKey'])

        try:
            region = ec2.get_region(doc['regionName'])
            ec2.connect_to_region(doc['regionName'])
            ec2.get_zone(doc['availabilityZone'])
            doc['regionHost'] = region.endpoint
        except EC2ResponseError:
            raise ValidationException('Invalid AWS credentials')
        except RegionDoesNotExist:
            raise ValidationException('Invalid region', 'regionName')
        except ZoneDoesNotExist:
            raise ValidationException('Invalid zone', 'availabilityZone')

        return doc

    def filter(self, profile, user):
        profile = super(Aws, self).filter(doc=profile, user=user)

        del profile['_accessLevel']
        del profile['_modelType']

        return profile

    def create_profile(self, userId, name, access_key_id, secret_access_key,
                       region_name, availability_zone):

        user = getCurrentUser()
        profile = {
            'name': name,
            'accessKeyId': access_key_id,
            'secretAccessKey': secret_access_key,
            'regionName': region_name,
            'availabilityZone': availability_zone,
            'userId': userId
        }

        profile = self.setUserAccess(profile, user, level=AccessType.ADMIN,
                                     save=False)

        return self.save(profile)

    def find_profiles(self, userId):
        query = {
            'userId': userId
        }

        return self.find(query)
