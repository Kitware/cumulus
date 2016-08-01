###############################################################################
#  Copyright 2016 Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the "License" );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################


class InstanceState():
    PENDING = 'pending'
    RUNNING = 'running'
    SHUTTINGDOWN = 'shutting-down'
    TERMINATED = 'terminated'
    STOPPING = 'stopping'
    STOPPED = 'stopped'


class CloudProvider(object):
    __provider_registry__ = {}

    def __new__(cls, profile):
        if cls == CloudProvider:
            assert 'cloudProvider' in profile, \
                'Profile does not have a "cloudProvider" attribute'

            try:
                subcls = cls.__provider_registry__[profile['cloudProvider']]
            except KeyError:
                raise NotImplementedError('No provider for %s profiles'
                                          % profile['cloudProvider'])

            return subcls(profile)

        return super(CloudProvider, cls).__new__(cls)

    def __init__(self, profile):
        self.girder_profile_id = profile.get('_id', None)

        for key, value in profile.items():
            setattr(self, key, value)

    @classmethod
    def register(cls, key, subcls):
        cls.__provider_registry__[key] = subcls

    def get_inventory(self):
        raise NotImplementedError('Must be implemented by subclass')

    def running_instances(self):
        raise NotImplementedError('Must be implemented by subclass')

    def get_master_instance(self):
        raise NotImplementedError('Must be implemented by subclass')

    def get_volumes(self):
        raise NotImplementedError('Must be implemented by subclass')

    def get_volume(self, volume_id):
        raise NotImplementedError('Must be implemented by subclass')

    def get_machine_images(self, **filters):
        """
        Return list of machine images that match filters.

        :param filters: The filters specifying the images to select.
        """
        raise NotImplementedError('Must be implemented by subclass')
