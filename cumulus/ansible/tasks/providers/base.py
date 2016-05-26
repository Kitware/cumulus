class InstanceState():
    PENDING = "pending"
    RUNNING = "running"
    SHUTTINGDOWN = "shutting-down"
    TERMINATED = "terminated"
    STOPPING = "stopping"
    STOPPED = "stopped"


class CloudProvider(object):
    __provider_registry__ = {}

    def __new__(cls, profile):
        if cls == CloudProvider:
            assert 'type' in profile, \
                'Profile does not have a "type" attribute'

            try:
                subcls = cls.__provider_registry__[profile['type']]
            except KeyError:
                raise NotImplementedError("No provider for %s profiles"
                                          % profile['type'])

            return subcls(profile)

        return super(CloudProvider, cls).__new__(cls, profile)

    def __init__(self, profile):
        self.girder_profile_id = profile.pop('_id', None)

        for key, value in profile.items():
            setattr(self, key, value)

    @classmethod
    def register(cls, key, subcls):
        cls.__provider_registry__[key] = subcls

    def get_inventory(self):
        raise NotImplementedError("Must be implemented by subclass")

    def get_master_instance(self):
        raise NotImplementedError("Must be implemented by subclass")

    def get_volumes(self):
        raise NotImplementedError("Must be implemented by subclass")

    def get_volume(self, volume_id):
        raise NotImplementedError("Must be implemented by subclass")
