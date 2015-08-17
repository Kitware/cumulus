from girder.utility.model_importer import ModelImporter
from girder.models.model_base import ValidationException
from girder.constants import AccessType
from cumulus.constants import VolumeType
from girder.api.rest import getCurrentUser


class AbstractVolumeAdapter(ModelImporter):
    """
    This defines the interface to be used by all volume adapters.
    """
    def __init__(self, volume):
        self.volume = volume

    def validate(self):
        """
        Adapters may implement this if they need to perform any validation
        steps whenever the volume info is saved to the database. It should
        return the document with any necessary alterations in the success case,
        or throw an exception if validation fails.
        """

        return self.volume

    def delete(self):
        """
        Adapters may implement this if they support a delete operation.
        """
        pass


class EbsVolumeAdapter(AbstractVolumeAdapter):

    def validate(self):

        valid_fs = ['ext2', 'ext3', 'ext4']

        if 'fs' in self.volume and self.volume['fs'] not in valid_fs:
            raise ValidationException('Unsupported file system type', 'fs')

        try:
            int(self.volume['size'])
        except ValueError:
            raise ValidationException('size number in an integer', 'size')

        # Name should be unique
        query = {
            'name': self.volume['name']
        }

        if '_id' in self.volume:
            query['_id'] = {'$ne': self.volume['_id']}

        user = getCurrentUser()
        volumes = self.model('volume', 'cumulus').find(query)
        volumes = self.model('volume', 'cumulus') \
            .filterResultsByPermission(volumes, user, AccessType.ADMIN, limit=1)

        if len(list(volumes)) > 0:
            raise ValidationException('A volume with that name already exists',
                                      'name')

        return self.volume


type_to_adapter = {
    VolumeType.EBS: EbsVolumeAdapter
}


def get_volume_adapter(volume):
    global type_to_adapter

    return type_to_adapter[volume['type']](volume)
