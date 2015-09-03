from jsonpath_rw import parse
from bson.objectid import ObjectId

from girder.constants import AccessType
from .base import BaseModel
from girder.models.model_base import ValidationException
from girder.api.rest import getCurrentUser


class Starclusterconfig(BaseModel):

    def __init__(self):
        super(Starclusterconfig, self).__init__()

    def initialize(self):
        self.name = 'starclusterconfigs'

    def validate(self, doc):
        profile_id = parse('aws.profileId').find(doc)

        if profile_id:
            profile_id = profile_id[0].value
            profile = self.model('aws', 'cumulus').load(profile_id,
                                                        user=getCurrentUser())

            if not profile:
                raise ValidationException('Invalid profile id')

        return doc

    def create(self, config):

        group = {
            '_id': ObjectId(self.get_group_id())
        }

        doc = self.setGroupAccess(config, group, level=AccessType.ADMIN,
                                  save=True)

        return doc
