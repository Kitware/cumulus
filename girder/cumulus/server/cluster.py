from girder.api.rest import Resource
from girder.api import access
import cherrypy
import json


class Cluster(Resource):

    def __init__(self):
        self.resourceName = 'clusters'
        self.route('POST', (), self.create)
        self.route('POST', (':id', 'log'), self.handle_log_record)

    @access.public
    def handle_log_record(self, id, params):
        return self.model('cluster', 'cumulus').add_log_record(id, json.load(cherrypy.request.body))

    @access.public
    def create(self, params):
        # TODO Findout how to get plugin name rather than hardcoding it
        return self.model('cluster', 'cumulus').create(params)
