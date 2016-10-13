import click
import ConfigParser
import girder_client
import time
import json

import logging
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('boto3').setLevel(logging.CRITICAL)


def key(name):
    """Produces a function that accesses an item

    :param name: the key name for a value on a dictionary
    :returns: A function that when applied to a dict returns value for that key
    :rtype: function

    """

    def _key(dictionary):
        """Wrapped function for accessing an attribute

        The attribute 'name' is defined in the enclosing closure.

        :param dictionary: an object
        :returns: Value of the 'name' attribute or ''
        """
        return dictionary.get(name, '')

    return _key


def attr(name):
    """Produces a function that accesses an attribute

    :param name: Name of an attribute
    :returns: A function that when applied to an instance returns the
              value for the attribute 'name'
    :rtype: function

    """

    def _attr(obj):
        """Wrapped function for accessing an attribute

        The attribute 'name' is defined in the enclosing closure.

        :param dictionary: an object
        :returns: Value of the 'name' attribute or ''
        """
        return getattr(obj, name) if hasattr(obj, name) else ''

    return _attr


def profile(profiles):
    profile_dict = {p['_id']: p['name'] for p in profiles}

    def _profile(instance):
        return profile_dict[instance['profileId']]

    return _profile



class ConfigParam(click.ParamType):
    """Takes a file string and produces a RawConfigParser object"""
    name = 'config'

    def convert(self, value, param, ctx):
        try:
            parser = ConfigParser.RawConfigParser()
            parser.optionxform = str
            parser.read(value)

            return parser
        except Exception as e:
            self.fail(str(e))

CONFIG_PARAM = ConfigParam()


def section_property(prefix, config="config"):
    """Creates a property for a specific configuration section

    This function will create a getter and a setter for a particular
    section of a configuration file. The setter will create a private
    variable of the form '_{prefix}_section' that holds the section string,
    then read the configuraiton and set attributes on the object for each of
    the key/value pairs in that section of the form {prefix}_{key}. The getter
    simply returns the private variable's value (or None if it is not yet set).
    For Example:
        # Assume the foolowing configuration in /path/to/config.cfg
        [my_girder_section]
        api_url = "http://127.0.0.1:8080"
        some_property = "foobar"

        class Foo(object):
            foo_section = section_propery('foo')

            def __init__(self, config, girder_section):
                self.config = config
                self.girder_section = girder_section

        >>> import ConfigParser
        >>> config = ConfigParser.RawConfigParser("/path/to/config.cfg")
        >>> config.read("/path/to/config.cfg")
        ['/path/to/config.cfg']

        >>> f = Foo(config, 'my_girder_section')
        >>> f.foo_api_url
        'http://127.0.0.1:8080'
        >>> f.foo_some_property
        'foobar'

    This allows us to specify an arbitrary section of a configuration file and
    make each of its key/value pairs available as attributes on the object with
    a known prefix. By default this assumes that the object's config will be
    available as an attribute 'config' on the object. This can by modified by
    passing a different string to the config keword argument.
    """
    private_variable = "_%s_section" % prefix

    def __section_getter(self):
        return getattr(self, private_variable) if hasattr(self, private_variable) else None

    def __section_setter(self, section):
        obj_config = getattr(self, config)

        if obj_config.has_section(section):
            setattr(self, private_variable, section)

            for key, value in obj_config.items(section):
                setattr(self, "%s_%s" % (prefix, key), value)
        else:
            raise RuntimeError(
                "Configuration does not have a '%s' section" % section)

    return property(__section_getter, __section_setter, None)


class Proxy(object):

    aws_section = section_property("aws")
    girder_section = section_property("girder")
    profile_section = section_property("profile")
    cluster_section = section_property("cluster")
    volume_section = section_property("volume")

    def __init__(self, config, aws_section="aws", girder_section='girder'):
        self.config = config

        self.aws_section = aws_section
        self.girder_section = girder_section

        self._client = girder_client.GirderClient(apiUrl=self.girder_api_url)

        self._client.authenticate(self.girder_user,
                                  self.girder_password)

    @property
    def user(self):
        if not hasattr(self, "_user"):
            self._user = None

        if self._user is None:
            setattr(self, "_user", self.get("user/me"))

        return self._user


    @property
    def profiles(self):
        r = self.get("user/%s/aws/profiles" % self.user['_id'])
        return r


    @property
    def profile(self):
        if not hasattr(self, "_profile"):
            self._profile = None

        if self._profile is None:
            # Create or get the profile
            self.profile = self.get_profile_body()

        return self._profile


    @profile.setter
    def profile(self, profile):
        for p in self.profiles:
            if p['name'] == profile['name']:
                logging.info("Using pre-existing profile: %s (%s)" % (p['name'], p['_id']))
                self._profile = p

                self.wait_for_status(
                    'user/%s/aws/profiles/%s/status' % (self.user['_id'], self.profile['_id']),
                    'available')
                return None

        # Profile could not be found, create it
        r = self.post('user/%s/aws/profiles' % self.user['_id'],
                      data=json.dumps(profile))

        self._profile = r
        logging.debug("Created profile %s: %s" % (r['_id'], r))

        self.wait_for_status(
            'user/%s/aws/profiles/%s/status' % (self.user['_id'], self.profile['_id']),
            'available')
        return None

    @profile.deleter
    def profile(self):
        for p in self.profiles:
            if p['name'] == self.profile_name:
                try:
                    r = self.delete("user/%s/aws/profiles/%s" %
                                    (self.user['_id'], p['_id']))
                    if hasattr(self, "_profile"):
                        del(self._profile)
                except girder_client.HttpError as e:
                    if e.status == 400:
                        raise RuntimeError(e.responseText)
                    else:
                        raise e
                return None

        logging.debug(
            "No profile with name '%s' found. Skipping" % self.profile_name)

        return None

    def get_profile_body(self):
        if self.profile_section:
            return {
                'accessKeyId': self.aws_access_key_id,
                'availabilityZone': self.aws_availabilityZone,
                'name': self.profile_name,
                'regionName': self.aws_regionName,
                'cloudProvider': self.profile_cloudProvider,
                'secretAccessKey': self.aws_secret_access_key
            }
        else:
            raise RuntimeError("No profile section found!")

    @property
    def volumes(self):
        r = self.get("volumes")
        return r

    @property
    def volume(self):
        if not hasattr(self, "_volume"):
            self._volume = None

        if self._volume is None:
            # Create or get the profile
            self.volume = self.get_volume_body()

        return self._volume

    @volume.setter
    def volume(self, volume):
        for v in self.volumes:
            if v['name'] == volume['name'] and \
               v['profileId'] == self.profile['_id']:
                logging.info("Using pre-existing volume: %s (%s)" % (v['name'], v['_id']))
                self._volume = v

                self.wait_for_status(
                    'volumes/%s/status' % (self.volume['_id']),
                    'created')
                return None

        # Volume could not be found, create it
        r = self.post('volumes', data=json.dumps(volume))

        self._volume = r
        logging.debug("Created volume %s: %s" % (r['_id'], r))

        self.wait_for_status(
            'volumes/%s/status' % (self.volume['_id']),
            'created')
        return None



    def get_volume_body(self):
        if self.profile_section:
            return {
                'name': self.volume_name,
                'profileId': self.profile['_id'],
                'size': self.volume_size,
                'type': self.volume_type,
                'zone': self.volume_zone
            }
        else:
            raise RuntimeError("No profile section found!")


    @property
    def clusters(self):
        r = self.get("clusters")
        return r

    @property
    def cluster(self):
        if not hasattr(self, "_cluster"):
            self._cluster = None

        if self._cluster is None:
            self.cluster = self.get_cluster_body()

        return self._cluster

    @cluster.setter
    def cluster(self, cluster):
        for c in self.clusters:
            if c['name'] == cluster['name']:
                logging.info("Using pre-existing cluster: %s (%s)" % (c['name'], c['_id']))
                self._cluster = c
                return None

        # Profile could not be found, create it
        r = self.post('clusters', data=json.dumps(cluster))

        self._cluster = r
        logging.debug("Created cluster %s: %s" % (r['_id'], r))
        return None


    @cluster.deleter
    def cluster(self):
        for c in self.clusters:
            if c['name'] == self.cluster_name:

                try:
                    r = self.delete("clusters/%s" % c['_id'])
                    if hasattr(self, "_cluster"):
                        del(self._cluster)
                except girder_client.HttpError as e:
                    if e.status == 400:
                        raise RuntimeError(e.responseText)
                    else:
                        raise e
                return None

        logging.debug(
            "No profile with name '%s' found. Skipping" % self.profile_name)

        return None



    def get_cluster_body(self):
        if self.cluster_section:
            return {
                'config': {
                    'launch': {
                        'spec': 'ec2',
                        'params': {
                            'master_instance_type': self.cluster_master_instance_type,
                            'master_instance_ami': self.cluster_master_instance_ami,
                            'node_instance_count': self.cluster_node_instance_count,
                            'node_instance_type': self.cluster_node_instance_type,
                            'node_instance_ami': self.cluster_node_instance_ami,
                            'terminate_wait_timeout': int(self.cluster_terminate_wait_timeout)
                        }
                    }
                },
                'profileId': self.profile['_id'],
                'name': self.cluster_name,
                'type': self.cluster_type
            }
        else:
            raise RuntimeError("No cluster section found!")

    def wait_for_status(self, status_url, status, timeout=10, log_url=None):
        logging.debug("Waiting for status '%s' at '%s'" % (status, status_url))

        if log_url is not None:
            r = self.get(log_url)
            log_offset = len(r['log'])
            logging.debug("Log offset set to: %s" % log_offset)

        start = time.time()
        while True:
            if log_url is not None:
                r = self.get(log_url, parameters={
                    'offset': log_offset
                })

                log_offset += len(r['log'])

                for entry in r['log']:
                    logging.debug("%s" % entry)
                    if entry['type'] == 'task':
                        if entry['status'] in ['finished', 'skipped', 'starting']:
                            logging.info("%s (%s)" % (entry['msg'], entry['status']))
                        elif entry['status'] == 'error':
                            logging.error("%s (%s)" % (entry['msg'], entry['status']))

            r = self.get(status_url)

            if r['status'] == status:
                break

            if r['status'] == 'error':
                if log_url is not None:
                    raise RuntimeError(
                        "Cluster has moved into an error state! "
                        "See: %s/%s for more information" % (self.girder_api_url, log_url))
                else:
                    raise RuntimeError(
                        "Operation moved resource into an error state!"
                        " See: %s/%s" % (self.girder_api_url, status_url))

            if time.time() - start > timeout:
                raise RuntimeError(
                    "Resource at '%s' never moved into the '%s' state, current"
                    " state is '%s'" % (status_url, status, r['status']))

            time.sleep(1)

    def launch_cluster(self, cluster, timeout=300):
        status_url = 'clusters/%s/status' % cluster['_id']
        log_url    = 'clusters/%s/log' % cluster['_id']

        r = self.put('clusters/%s/launch' % cluster['_id'])

        self.wait_for_status(status_url, 'running',
                             timeout=timeout,
                             log_url=log_url)

    def terminate_cluster(self, cluster, timeout=300):
        status_url = 'clusters/%s/status' % cluster['_id']
        log_url    = 'clusters/%s/log' % cluster['_id']

        r = self.put('clusters/%s/terminate' % cluster['_id'])

        self.wait_for_status(status_url, 'terminated',
                             timeout=timeout,
                             log_url=log_url)

    def get_instances(self):
        import boto3

        ec2 = boto3.resource(
            'ec2', aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)

        for instance in ec2.instances.all():
            yield instance

    def get(self, uri, **kwargs):
        url = "%s/%s" % (self.girder_api_url, uri)
        logging.debug('GET %s' % url)
        if kwargs:
            logging.debug(kwargs)

        return self._client.get(uri, **kwargs)

    def post(self, uri, **kwargs):
        url = "%s/%s" % (self.girder_api_url, uri)
        logging.debug('POST %s' % url)
        if kwargs:
            logging.debug(kwargs)

        return self._client.post(uri, **kwargs)

    def put(self, uri, **kwargs):
        url = "%s/%s" % (self.girder_api_url, uri)
        logging.debug('PUT %s' % url)
        if kwargs:
            logging.debug(kwargs)

        return self._client.put(uri, **kwargs)

    def delete(self, uri, **kwargs):
        url = "%s/%s" % (self.girder_api_url, uri)
        logging.debug('DELETE %s' % url)
        if kwargs:
            logging.debug(kwargs)

        return self._client.delete(uri, **kwargs)
