import cherrypy
import re
from jsonpath_rw import parse
import urllib2
from bson.objectid import ObjectId

from girder.api import access
from girder.api.describe import Description
from girder.constants import AccessType
from girder.api.docs import addModel
from girder.api.rest import RestException, getCurrentUser, getBodyJson
from girder.models.model_base import ValidationException
from girder.api.rest import loadmodel
from .base import BaseResource

import starcluster.config

from cumulus.constants import VolumeType
from cumulus.constants import VolumeState
from cumulus.constants import ClusterType
from cumulus.common import get_config_url


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

    def _clean(self, volume):
        del volume['access']
        volume['_id'] = str(volume['_id'])

        return volume

    def _create_ebs(self, volume_id, body, zone):
        user = self.getCurrentUser()
        name = body['name']
        size = body['size']
        fs = body.get('fs', None)
        config = body['config']

        return self.model('volume', 'cumulus').create_ebs(user, config,
                                                          volume_id, name,
                                                          zone, size, fs)

    def _create_config_request(self, config_id):
        base_url = re.match('(.*)/volumes.*', cherrypy.url()).group(1)
        config_url = get_config_url(base_url, config_id)

        headers = {
            'Girder-Token': self.get_task_token()['_id']
        }

        config_request = urllib2.Request(config_url, headers=headers)

        return config_request

    @access.user
    def create(self, params):
        body = getBodyJson()
        self.requireParams(['name', 'type', 'size', 'config'], body)
        self.requireParams(['_id'], body['config'])

        if not VolumeType.is_valid_type(body['type']):
                raise RestException('Invalid volume type.', code=400)

        config_id = parse('config._id').find(body)[0].value
        config_request = self._create_config_request(config_id)

        config = starcluster.config.StarClusterConfig(config_request)
        config.load()
        ec2 = config.get_easy_ec2()

        if 'zone' in body:
            # Check that the zone is valid
            try:
                zone = body['zone']
                ec2.get_zone(zone)
            except starcluster.exception.ZoneDoesNotExist:
                raise ValidationException('Zone does not exist in region',
                                          'zone')

        # Just pick the first
        else:
            zone = ec2.get_zones()[0].name

        vol = ec2.create_volume(body['size'], zone)

        volume = self._create_ebs(vol.id, body, zone)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] = '/volumes/%s' % volume['_id']
        self._clean(volume)

        return volume

    addModel('ConfigParameter', {
        'id': 'ConfigParameter',
        'required': ['_id'],
        'properties': {
            '_id': {'type': 'string',
                    'description': 'Id of starcluster configuration'}
        }
    })

    addModel('VolumeParameters', {
        'id': 'VolumeParameters',
        'required': ['name', 'config', 'type', 'zone', 'size'],
        'properties': {
            'name': {'type': 'string',
                     'description': 'The name to give the cluster.'},
            'config':  {'type': 'ConfigParameter',
                        'description': 'The starcluster configuration'},
            'type': {'type': 'string',
                     'description': 'The type of volume to create ( currently '
                     'only esb )'},
            'zone': {'type': 'string',
                     'description': 'The availability region'},
            'size': {'type': 'integer',
                     'description': 'The size of the volume to create'}
        },
    })

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
        self._clean(volume)

        return volume

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

        return [self._clean(volume) for volume in volumes]

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
        vol.append({
            volume['name']: {
                'volume_id': volume['ec2']['id'],
                'mount_path': path
            }
        })

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

        config_id = parse('config._id').find(volume)[0].value
        volume_id = parse('ec2.id').find(volume)[0].value
        config_request = self._create_config_request(config_id)
        conf = starcluster.config.StarClusterConfig(config_request)
        conf.load()
        status = self._get_status(conf, volume_id)

        if status != VolumeState.AVAILABLE:
            raise RestException('This volume is not available to attach '
                                'to a cluster',
                                400)

        if cluster['status'] == 'running':
            raise RestException('Unable to attach volume to running cluster',
                                400)

        volumes = cluster.setdefault('volumes', [])
        volumes.append(volume['_id'])

        # Now update the configuration to include this volume
        starcluster_config = self.model('starclusterconfig',
                                        'cumulus').load(config_id, force=True)
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
    })

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
        user = getCurrentUser()
        config_id = parse('config._id').find(volume)[0].value
        volume_id = parse('ec2.id').find(volume)[0].value
        config_request = self._create_config_request(config_id)
        conf = starcluster.config.StarClusterConfig(config_request)
        conf.load()
        status = self._get_status(conf, volume_id)

        if status != VolumeState.INUSE:
            raise RestException('Volume is not attached', 400)

        # Call ec2 to do the detach
        vol = conf.get_easy_ec2().get_volume(volume_id)
        vol.detach()

        # First remove from cluster
        cluster = self.model('cluster', 'cumulus').load(volume['clusterId'],
                                                        user=user,
                                                        level=AccessType.ADMIN)
        cluster['volumes'] = cluster.get('volumes').remove(volume['_id'])
        del volume['clusterId']

        # Now remove from starcluster configuration
        starcluster_config = self.model('starclusterconfig', 'cumulus') \
            .load(config_id, force=True)
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

        self.model('volume', 'cumulus').remove(volume)

    delete.description = (
        Description('Delete a volume')
        .param('id', 'The volume id.', paramType='path', required=True))

    def _get_status(self, starcluster_config, volume_id):
        ec2 = starcluster_config.get_easy_ec2()
        v = ec2.get_volume(volume_id)

        return v.update()

    @access.user
    @loadmodel(model='volume', plugin='cumulus', level=AccessType.ADMIN)
    def get_status(self, volume, params):

        ec2_id = parse('ec2.id').find(volume)[0].value

        if len(ec2_id) < 1:
            return {'status': 'creating'}

        # If we have an ec2 id delegate the call to ec2
        config_id = parse('config._id').find(volume)[0].value
        config_request = self._create_config_request(config_id)
        conf = starcluster.config.StarClusterConfig(config_request)
        conf.load()

        return {'status': self._get_status(conf, ec2_id)}

    get_status.description = (
        Description('Get the status of a volume')
        .param('id', 'The volume id.', paramType='path', required=True))
