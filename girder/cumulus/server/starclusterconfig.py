import io
import cherrypy
import json
from jinja2 import Template
from jsonpath_rw import parse

from girder.api import access
from girder.api.describe import Description
from ConfigParser import ConfigParser
from girder.api.docs import addModel
from girder.constants import AccessType
from girder.api.rest import RestException, getBodyJson
from .base import BaseResource
import cumulus


class StarClusterConfig(BaseResource):

    def __init__(self):
        self.resourceName = 'starcluster-configs'
        self.route('POST', (), self.create)
        self.route('GET', (':id',), self.get)
        self.route('PATCH', (':id', 'import'), self.import_config)
        self.route('DELETE', (':id',), self.delete)
        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('starclusterconfig', 'cumulus')

    def _clean(self, config):
        del config['access']

        return config

    @access.user
    def import_config(self, id, params):
        user = self.getCurrentUser()
        config_parser = ConfigParser()
        content = cherrypy.request.body.read()
        config_parser.readfp(io.BytesIO(content))

        valid_sections = ['global', 'key', 'aws', 'cluster', 'permission',
                          'plugin', 'vol']

        config = {}

        for section in config_parser.sections():
            parts = filter(None, section.split(' '))

            section_name = None

            if len(parts) == 1:
                section_type = parts[0]
            if len(parts) == 2:
                (section_type, section_name) = parts

            if section_type in valid_sections:
                section_list = []
                if section_type not in config:
                    config[section_type] = section_list
                else:
                    section_list = config[section_type]

                options = {}
                for (name, value) in config_parser.items(section):
                    options[name] = value

                if section_name:
                    section_list.append({section_name: options})
                else:
                    config[section_type] = options

        star_config = self._model.load(id, user=user, level=AccessType.ADMIN)
        star_config['config'] = config
        self._model.save(star_config)

        return self._clean(star_config)

    import_config.description = (
        Description('Import star cluster configuration in ini format')
        .param(
            'id',
            'The config to upload the configuration to',
            required=True, paramType='path')
        .param(
            'body',
            'The contents of the INI file',
            required=True, paramType='body')
        .consumes('text/plain'))

    @access.user
    def create(self, params):
        body = getBodyJson()
        self.requireParams(['name'], body)
        user = self.getCurrentUser()

        self.check_group_membership(user, cumulus.config.girder.group)

        config = self._model.create(body)

        cherrypy.response.status = 201
        cherrypy.response.headers['Location'] \
            = '/starcluster-configs/%s' % config['_id']

        return self._clean(config)

    addModel("Global", {
        "id": "Global",
        "properties": {
            "default_template": {
                "type": "string"
            }
        }
    })

    addModel("StarClusterSetting", {
        "id": "StarClusterSetting",
        "type": "object",
        "additionalProperties": {
            "type": "string"
        }
    })

    addModel("Section", {
        "id": "Section",
        "type": "object",
        "additionalProperties": {
            "type": "array",
            "items": {
                "$ref": "StarClusterSetting"
            }
        }
    })

    addModel('StarClusterConfig', {
        "id": "StartClusterConfig",
        "required": "global",
        "properties": {
            "global": {
                "type": "Global",
            },
            "key": {
                "type": "array",
                "items": {
                    "$ref": "Section"
                }
            },
            "aws": {
                "type": "array",
                "items": {
                    "$ref": "Section"
                }
            },
            "cluster": {
                "type": "array",
                "items": {
                    "$ref": "Section"
                }
            },
            "permission": {
                "type": "array",
                "items": {
                    "$ref": "Section"
                }
            },
            "plugin": {
                "type": "array",
                "items": {
                    "$ref": "Section"
                }
            }
        }
    })

    addModel('NamedStarClusterConfig', {
        "id": "NamedStarClusterConfig",
        "required": ["name"],
        "properties": {
            "name": {"type": "string",
                     "description": "The name of the configuration."},
            "config":  {"type": "StarClusterConfig",
                        "description": "The JSON configuration."}
        }})

    create.description = (
        Description('Create cluster configuration')
        .param(
            'body',
            'The JSON configuation ',
            required=True, paramType='body', dataType='NamedStarClusterConfig'))

    @access.user
    def get(self, id, params):
        user = self.getCurrentUser()

        format = 'json'

        if 'format' in params:
            format = params['format']

        doc = self._model.load(id, user=user, level=AccessType.READ)

        if not doc:
            raise RestException('Config not found', code=404)

        config = doc['config']

        # If we have a aws profile apply it to the configuration
        profile_id = parse('aws.profileId').find(doc)

        if profile_id:
            profile_id = profile_id[0].value
            profile = self.model('aws', 'cumulus').load(profile_id, user=user)

            json_str = json.dumps(config)
            json_str = Template(json_str).render(**profile)
            config = json.loads(json_str)

        if format == 'json':
            return config
        else:
            def stream():
                cherrypy.response.headers['Content-Type'] = 'text/plain'

                for (type, sections) in config.iteritems():
                    section_config = ""

                    if type == 'global':
                        section_config += '[global]\n'
                        for (k, v) in sections.iteritems():
                            section_config += '%s = %s\n' % (k, v)
                        yield section_config
                        continue
                    for section in sections:
                        (name, values) = section.iteritems().next()
                        section_config += '[%s %s]\n' % (type, name)
                        for (k, v) in values.iteritems():
                            section_config += '%s = %s\n' % (k, v)
                    yield section_config

            return stream

    get.description = (
        Description('Get configuration')
        .param(
            'id',
            'The id of the config to fetch',
            required=True, paramType='path')
        .param(
            'format',
            'The format to fetch in "json" or "ini".',
            required=False, paramType='query'))

    @access.user
    def delete(self, id, params):
        user = self.getCurrentUser()
        config = self._model.load(id, user=user, level=AccessType.ADMIN)

        self._model.remove(config)

    delete.description = (
        Description('Delete a starcluster configuration')
        .param(
            'id',
            'The starcluster configuration id.', paramType='path',
            required=True))
