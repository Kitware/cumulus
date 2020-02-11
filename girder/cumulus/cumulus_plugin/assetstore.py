from girder.api import access
from girder.api.describe import Description, autoDescribeRoute
from girder.models.assetstore import Assetstore

@access.user
@autoDescribeRoute(
    Description('Retrieve assetstores with a given name.')
    .param('name', 'The assetstore name', required=True)
    .errorResponse())
def lookupAssetstore(name, params):
    return Assetstore().find({'name': name})
