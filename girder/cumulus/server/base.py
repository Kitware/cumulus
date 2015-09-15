from girder.api.rest import Resource
from cumulus.common.girder import get_task_token, check_group_membership


class BaseResource(Resource):

    def check_group_membership(self, user, group):
        return check_group_membership(user, group)

    def get_task_token(self):
        return get_task_token()
