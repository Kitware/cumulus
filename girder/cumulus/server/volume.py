import cherrypy
from jsonpath_rw import parse
from bson.objectid import ObjectId

from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType
from girder.api.docs import addModel
from girder.api.rest import RestException, getCurrentUser, getBodyJson
from girder.models.model_base import ValidationException
from girder.api.rest import loadmodel
from .base import BaseResource

from starcluster.exception import VolumeDoesNotExist, ZoneDoesNotExist

from cumulus.constants import VolumeType
from cumulus.constants import VolumeState
from cumulus.constants import ClusterType
from cumulus.starcluster.common import get_easy_ec2


class Volume(BaseResource):

    def __init__(self):
        self.resourceName = 'volumes'
        self.route('POST', (), self.create)
        self.route('GET', (':id', ), self.get)
        self.route('GET', (), self.find)
        self.route('GET', (':id', 'status'), self.get_status)
        self.route('PUT', (':id', 'clusters', ':clusterId', 'attach'),
                   self.attach)
        self.route('PUT', (':id', 'detach'), self.detach)
        self.route('DELETE', (':id', ), self.delete)

        self._model = self.model('volume', 'cumulus')

    def _create_ebs(self, body, zone):
        user = self.getCurrentUser()
        name = body['name']
        size = body['size']
        fs = body.get('fs', None)
        profileId = body['aws']['profileId']

        return self.model('volume', 'cumulus').create_ebs(user, profileId, name,
                                                          zone, size, fs)

    @access.user
    def create(self, params):
        body = getBodyJson()
        self.requireParams(['name', 'type', 'size', 'aws'], body)
        self.requireParams(['profileId'], body['aws'])

        if not VolumeType.is_valid_type(body['type']):
                raise RestException('Invalid volume type.', code=400)

        profile_id = parse('aws.profileId').find(body)
        if not profile_id:
            raise RestException('A profile id must be provided', 400)

        profile_id = profile_id[0].value

        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())
        if not profile:
            raise RestException('Invalid profile', 400)

        ec2 = get_easy_ec2(profile)

        if 'zone' in body:
            # Check that the zone is valid
            try:
                zone = body['zone']
                ec2.get_zone(zone)
            except ZoneDoesNotExist:
                raise ValidationException('Zone does not exist in region',
                                          'zone')

        # Use the zone from the profile
        else:
            zone = profile['availabilityZone']

        volume = self._create_ebs(body, zone)
        vol = ec2.create_volume(body['size'], zone)
        # Now set the EC2 volume id
        volume['ec2']['id'] = vol.id
        self.model('volume', 'cumulus').save(volume)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/volumes/%s' % volume['_id']

        return self._model.filter(volume, getCurrentUser())

    addModel('AwsParameter', {
        'id': 'ConfigParameter',
        'required': ['_id'],
        'properties': {
            'profileId': {'type': 'string',
                          'description': 'Id of AWS profile to use'}
        }
    }, 'volumes')

    addModel('VolumeParameters', {
        'id': 'VolumeParameters',
        'required': ['name', 'config', 'type', 'zone', 'size'],
        'properties': {
            'name': {'type': 'string',
                     'description': 'The name to give the cluster.'},
            'aws':  {'type': 'AwsParameter',
                     'description': 'The AWS configuration'},
            'type': {'type': 'string',
                     'description': 'The type of volume to create ( currently '
                     'only esb )'},
            'zone': {'type': 'string',
                     'description': 'The availability region'},
            'size': {'type': 'integer',
                     'description': 'The size of the volume to create'}
        },
    }, 'volumes')

    create.description = (
        Description('Create a volume')
        .param(
            'body',
            'The properties to use to create the volume.',
            dataType='VolumeParameters',
            required=True, paramType='body'))

    @access.user
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.READ)
    def get(self, volume, params):

        return self._model.filter(volume, getCurrentUser())

    get.description = (
        Description('Get a volume')
        .param(
            'id',
            'The volume id.', paramType='path', required=True))

    @access.user
    def find(self, params):
        user = self.getCurrentUser()
        query = {}

        if 'clusterId' in params:
            query['clusterId'] = ObjectId(params['clusterId'])

        limit = params.get('limit', 50)

        volumes = self.model('volume', 'cumulus').find(query=query)
        volumes = list(volumes)

        volumes = self.model('volume', 'cumulus') \
            .filterResultsByPermission(volumes, user, AccessType.ADMIN,
                                       limit=int(limit))

        return [self._model.filter(volume, user) for volume in volumes]

    find.description = (
        Description('Search for volumes')
        .param('limit', 'The max number of volumes to return',
               paramType='query', required=False, default=50))

    def _add_volume(self, config, cluster, volume, path):
        # Check that we don't already have a volume with this name
        vol = config.setdefault('vol', [])
        for v in vol:
            if volume['name'] in v:
                raise ValidationException('A volume with that name is already '
                                          'exists', 'name')

        # Add to volume list
        volume_config = {
            'volume_id': volume['ec2']['id'],
            'mount_path': path
        }

        vol.append({
            volume['name']: volume_config
        })

        if 'fs' in volume:
            volume_config['fs'] = volume['fs']

        template_name = cluster['template']
        # Now add the volume to the cluster
        for c in config['cluster']:
            if template_name in c:
                current_volumes = c[template_name].get('volumes', '')
                current_volumes = [x for x in current_volumes.split(',') if x]

                if volume['name'] in current_volumes:
                    raise ValidationException('A volume with that name is '
                                              'already attached', 'name')
                current_volumes.append(volume['name'])

                c[template_name]['volumes'] = ','.join(current_volumes)
                break

        return config

    def _remove_volume(self, config, cluster, volume):
        # Check that we don't already have a volume with this name
        vol = config.get('vol', [])
        to_delete = None
        for v in vol:
            if volume['name'] in v:
                to_delete = v
                break

        vol.remove(to_delete)

        # Now remove the volume from the cluster
        template_name = cluster['template']
        for c in config['cluster']:
            if template_name in c:
                current_volumes = c[template_name].get('volumes', '')
                current_volumes = [x for x in current_volumes.split(',') if x]
                current_volumes.remove(volume['name'])
                c['default_cluster']['volumes'] = ','.join(current_volumes)
                if not c['default_cluster']['volumes']:
                    del c['default_cluster']['volumes']
                break

        return config

    @access.user
    @loadmodel(map={'clusterId': 'cluster'}, model='cluster', plugin='cumulus',
               level=AccessType.ADMIN)
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def attach(self, volume, cluster, params):
        body = getBodyJson()
        self.requireParams(['path'], body)

        if cluster['type'] != ClusterType.EC2:
            raise RestException('Invalid cluster type', 400)

        profile_id = parse('aws.profileId').find(volume)[0].value
        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())
        ec2 = get_easy_ec2(profile)
        volume_id = parse('ec2.id').find(volume)[0].value
        status = self._get_status(ec2, volume_id)

        if status != VolumeState.AVAILABLE:
            raise RestException('This volume is not available to attach '
                                'to a cluster',
                                400)

        if cluster['status'] == 'running':
            raise RestException('Unable to attach volume to running cluster',
                                400)

        volumes = cluster.setdefault('volumes', [])
        volumes.append(volume['_id'])

        # Now update the configuration for the cluster to include this volume
        cluster_config_id = parse('config._id').find(cluster)[0].value
        starcluster_config = self.model('starclusterconfig',
                                        'cumulus').load(cluster_config_id,
                                                        force=True)
        self._add_volume(starcluster_config['config'], cluster, volume,
                         body['path'])

        # Add cluster id to volume
        volume['clusterId'] = cluster['_id']

        self.model('starclusterconfig', 'cumulus').save(starcluster_config)
        self.model('cluster', 'cumulus').save(cluster)
        self.model('volume', 'cumulus').save(volume)

    addModel('AttachParameters', {
        'id': 'AttachParameters',
        'required': ['path'],
        'properties': {
            'path': {'type': 'string',
                     'description': 'Id of starcluster configuration'}
        }
    }, 'volumes')

    attach.description = (
        Description('Attach a volume to a cluster')
        .param(
            'id',
            'The id of the volume to attach', required=True,
            paramType='path')
        .param(
            'clusterId',
            'The cluster to attach the volume to.', required=True,
            paramType='path')
        .param(
            'body',
            'The properties to template on submit.',
            dataType='AttachParameters',
            paramType='body'))

    @access.user
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def detach(self, volume, params):

        if 'clusterId' not in volume:
            raise RestException('Volume is not attached', 400)

        user = getCurrentUser()
        profile_id = parse('aws.profileId').find(volume)[0].value
        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())
        ec2 = get_easy_ec2(profile)
        volume_id = parse('ec2.id').find(volume)[0].value
        status = self._get_status(ec2, volume_id)

        # Call ec2 to do the detach
        if status == VolumeState.INUSE:
            try:
                vol = ec2.get_volume(volume_id)
                vol.detach()
            except VolumeDoesNotExist:
                pass

        # First remove from cluster
        cluster = self.model('cluster', 'cumulus').load(volume['clusterId'],
                                                        user=user,
                                                        level=AccessType.ADMIN)
        cluster.setdefault('volumes', []).remove(volume['_id'])
        del volume['clusterId']

        # Now remove from starcluster configuration for this cluster
        cluster_config_id = parse('config._id').find(cluster)[0].value
        starcluster_config = self.model('starclusterconfig', 'cumulus') \
            .load(cluster_config_id, force=True)
        self._remove_volume(starcluster_config['config'], cluster, volume)

        self.model('starclusterconfig', 'cumulus').save(starcluster_config)
        self.model('cluster', 'cumulus').save(cluster)
        self.model('volume', 'cumulus').save(volume)

    detach.description = (
        Description('Detach a volume from a cluster')
        .param(
            'id',
            'The id of the attached volume', required=True,
            paramType='path'))

    @access.user
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def delete(self, volume, params):
        if 'clusterId' in volume:
            raise RestException('Unable to delete attached volume')

        # Call EC2 to delete volume
        profile_id = parse('aws.profileId').find(volume)[0].value
        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())
        ec2 = get_easy_ec2(profile)
        volume_id = parse('ec2.id').find(volume)[0].value
        vol = ec2.get_volume(volume_id)
        vol.delete()

        self.model('volume', 'cumulus').remove(volume)

    delete.description = (
        Description('Delete a volume')
        .param('id', 'The volume id.', paramType='path', required=True))

    def _get_status(self, ec2, volume_id):
        v = ec2.get_volume(volume_id)

        return v.update()

    @access.user
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def get_status(self, volume, params):

        ec2_id = parse('ec2.id').find(volume)[0].value

        if len(ec2_id) < 1:
            return {'status': 'creating'}

        # If we have an ec2 id delegate the call to ec2
        profile_id = parse('aws.profileId').find(volume)[0].value
        profile = self.model('aws', 'cumulus').load(profile_id,
                                                    user=getCurrentUser())
        ec2 = get_easy_ec2(profile)

        return {'status': self._get_status(ec2, ec2_id)}

    get_status.description = (
        Description('Get the status of a volume')
        .param('id', 'The volume id.', paramType='path', required=True))
