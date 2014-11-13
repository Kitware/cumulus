from girder.models.model_base import AccessControlledModel
import cumulus

class BaseModel(AccessControlledModel):

    def initialize(self):
        group = self.model('group').find({'name': cumulus.config.girder.group})

        if group.count() != 1:
            raise Exception('Unable to load group "%s"' % cumulus.config.girder.group)

        self._group_id = group.next()['_id']