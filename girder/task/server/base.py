from girder.api.rest import Resource, RestException
from bson.objectid import ObjectId
import cumulus


class BaseResource(Resource):

    def _get_group_id(self, group):
        group = self.model('group').find({'name': group})

        if group.count() != 1:
            raise Exception('Unable to load group "%s"' % group)

        return group.next()['_id']

    def check_group_membership(self, user, group):
        group_id = self._get_group_id(group)
        if 'groups' not in user or ObjectId(group_id) not in user['groups']:
            raise RestException('The user is not in the required group.',
                                code=403)

    def get_task_token(self):
        user = self.model('user').find({'login': cumulus.config.girder.user})

        if user.count() != 1:
            raise Exception('Unable to load user "%s"'
                            % cumulus.config.girder.user)

        user = user.next()

        return self.model('token').createToken(user=user, days=7)
