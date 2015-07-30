from girder.constants import AccessType
from bson.objectid import ObjectId
from .base import BaseModel


class Starclusterconfig(BaseModel):

    def __init__(self):
        super(Starclusterconfig, self).__init__()

    def initialize(self):
        self.name = 'starclusterconfigs'

    def validate(self, doc):
        return doc

    def create(self, config):

        group = {
            '_id': ObjectId(self.get_group_id())
        }

        doc = self.setGroupAccess(config, group, level=AccessType.ADMIN,
                                  save=True)

        return doc
