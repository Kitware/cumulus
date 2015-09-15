from bson.objectid import ObjectId

from girder.models.model_base import ValidationException
from girder.constants import AccessType

from .base import BaseModel
from ..utility.volume_adapters import get_volume_adapter
from cumulus.constants import VolumeType


class Volume(BaseModel):

    def __init__(self):
        super(Volume, self).__init__()

    def initialize(self):
        self.name = 'volumes'

        self.exposeFields(level=AccessType.READ,
                          fields=('_id', 'config', 'ec2', 'fs', 'name', 'size',
                                  'type', 'zone', 'aws'))

    def filter(self, vol, user):
        vol = super(Volume, self).filter(doc=vol, user=user)
        del vol['_accessLevel']
        del vol['_modelType']
        vol['_id'] = str(vol['_id'])

        return vol

    def validate(self, volume):
        if not volume['name']:
            raise ValidationException('Name must not be empty.', 'name')

        if not volume['type']:
            raise ValidationException('Type must not be empty.', 'type')

        volume_adapter = get_volume_adapter(volume)
        volume = volume_adapter.validate()

        return volume

    def create_ebs(self, user, profileId, name, zone, size, fs):
        volume = {
            'name': name,
            'zone': zone,
            'size': size,
            'type': VolumeType.EBS,
            'ec2': {
                'id': None
            },
            'aws': {
                'profileId': profileId
            }
        }

        if fs:
            volume['fs'] = fs

        # Add userId field to make search for a user volumes easier
        volume['userId'] = user['_id']

        self.setUserAccess(volume, user=user, level=AccessType.ADMIN)
        group = {
            '_id': ObjectId(self.get_group_id())
        }
        self.setGroupAccess(volume, group, level=AccessType.ADMIN)

        self.save(volume)

        return volume
