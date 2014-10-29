import io
import cherrypy
from girder.api.rest import Resource
from girder.api import access
from girder.api.describe import Description
from ConfigParser import ConfigParser



class StarClusterConfig(Resource):

    def __init__(self):
        self.resourceName = 'starcluster-configs'
        self.route('POST', ('content',), self.import_file)
        self.route('GET', (':id',), self.get)

        # TODO Findout how to get plugin name rather than hardcoding it
        self._model = self.model('starclusterconfig', 'cumulus')

    @access.user
    def import_file(self, params):
        user = self.getCurrentUser()

        config_parser = ConfigParser()

        name = params['name']

        content = cherrypy.request.body.read()

        config_parser.readfp(io.BytesIO(content))

        valid_sections = ['global', 'key', 'aws', 'cluster', 'permission', 'plugin_section']

        config = {}

        for section in config_parser.sections():
            parts = section.split(' ')

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

        return {'id': self._model.create(user, name, config)}


    import_file.description = (Description(
            'Upload cluster configuration'
        )
        .param(
            'name',
            'Human readable name of configuration.',
            required=True, paramType='query')
        .param(
            'content',
            'The contents of the INI file',
            required=True, paramType='body'))

    @access.user
    def get(self, id, params):
        user = self.getCurrentUser()

        format = 'json'

        if 'format' in params:
            format = params['format']

        config = self._model.get(user, id)

        if format == 'json':
            return config['config']
        else:
            def stream():
                cherrypy.response.headers['Content-Type'] = 'text/plain'

                for (type, sections) in config['config'].iteritems():
                    section_config = ""

                    if type == 'global':
                        section_config +=  '[global]\n'
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

    get.description = (Description(
            'Get configuration'
        )
        .param(
            'id',
            'The id of the config to fetch',
            required=True, paramType='path')
        .param(
            'format',
            'The format to fetch in "json" or "ini".',
            required=False, paramType='query'))