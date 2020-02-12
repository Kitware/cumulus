from girder.api import access
from girder.api.describe import Description, autoDescribeRoute
from girder.api.rest import getCurrentUser
from girder.constants import TokenScope
from girder.models.assetstore import Assetstore

@access.user(scope=TokenScope.DATA_READ)
@autoDescribeRoute(
    Description('Retrieve assetstores with a given name.')
    .param('name', 'The assetstore name', required=True)
    .errorResponse())
def lookupAssetstore(name, params):
    assetstores = Assetstore().find({'name': name})
    user = getCurrentUser()
    if user.get('admin', False):
        return assetstores
    else:
        safe_only = lambda assetstore : {
            field: assetstore[field] for field in ('_id', 'name', 'type')
        }
        return list(map(safe_only, assetstores))
