from girder.models.model_base import AccessControlledModel
import cumulus


class BaseModel(AccessControlledModel):

    def __init__(self):
        super(BaseModel, self).__init__()
        self._group_id = None

    def get_group_id(self):

        if not self._group_id:
            group = self.model('group').find({
                'name': cumulus.config.girder.group
            })

            if group.count() != 1:
                raise Exception('Unable to load group "%s"'
                                % cumulus.config.girder.group)

            self._group_id = group.next()['_id']

        return self._group_id

    def filter(self, doc, user):
        doc = super(BaseModel, self).filter(doc=doc, user=user)
        doc.pop('_accessLevel', None)
        doc.pop('_modelType', None)
        if '_id' in doc:
            doc['_id'] = str(doc['_id'])

        return doc
