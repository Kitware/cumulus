from girder.constants import AccessType
from .base import BaseModel


class Script(BaseModel):

    def __init__(self):
        super(Script, self).__init__()

    def initialize(self):
        self.name = 'scripts'

    def validate(self, doc):
        return doc

    def create(self, user, script):

        doc = self.setUserAccess(script, user, level=AccessType.ADMIN,
                                 save=True)

        return doc
