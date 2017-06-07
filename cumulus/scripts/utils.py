import six
import click
import ConfigParser
import girder_client
import time
import json

import logging
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)


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


def get_profile(profiles):
    profile_dict = {p['_id']: p['name'] for p in profiles}

    def _profile(instance):
        try:
            return profile_dict[instance['profileId']]
        except KeyError:
            return ''

    return _profile


def aws_name_from_tag(resource):
    try:
        for tag in resource.tags:
            if tag['Key'] == 'Name':
                return tag['Value']
        return ''
    except Exception:
        return ''


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


def section_property(prefix, config='config'):
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
        api_url = 'http://127.0.0.1:8080'
        some_property = 'foobar'

        class Foo(object):
            foo_section = section_propery('foo')

            def __init__(self, config, girder_section):
                self.config = config
                self.girder_section = girder_section

        >>> import ConfigParser
        >>> config = ConfigParser.RawConfigParser('/path/to/config.cfg')
        >>> config.read('/path/to/config.cfg')
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
    private_variable = '_%s_section' % prefix

    def __section_getter(self):
        return getattr(self, private_variable) \
            if hasattr(self, private_variable) else None

    def __section_setter(self, section):
        if section is None:
            return

        obj_config = getattr(self, config)

        if obj_config.has_section(section):
            setattr(self, private_variable, section)

            for key, value in obj_config.items(section):
                setattr(self, '%s_%s' % (prefix, key), value)
        else:
            raise RuntimeError(
                'Configuration does not have a "%s" section' % section)

    return property(__section_getter, __section_setter, None)


class Proxy(object):

    aws_section = section_property('aws')
    girder_section = section_property('girder')
    profile_section = section_property('profile')
    cluster_section = section_property('cluster')
    volume_section = section_property('volume')

    def __init__(self, config, aws_section='aws', girder_section='girder'):
        self.verbose = 0
        self.config = config

        self.aws_section = aws_section
        self.girder_section = girder_section

        self.client = girder_client.GirderClient(apiUrl=self.girder_api_url)

        self.client.authenticate(self.girder_user,
                                 self.girder_password)

    def get_folder_id(self, path, create=True, parent=None, _type='folder'):
        if parent is None:
            parent = self.user['_id']
            _type = 'user'

        for part in path.split('/'):
            try:
                folder = self.client.listFolder(
                    parent, name=part, parentFolderType=_type).next()
            except StopIteration:
                if create:
                    folder = self.client.createFolder(
                        parent, part, parentType=_type)
                else:
                    return None

            _type = 'folder'
            parent = folder['_id']

        return parent

    @property
    def user(self):
        if not hasattr(self, '_user'):
            self._user = None

        if self._user is None:
            setattr(self, '_user', self.get('user/me'))

        return self._user

    def remote_profile(self, name=None):
        if name is None:
            name = self.profile_name

        for p in self.profiles:
            if p['name'] == name:
                return p

        return None

    @property
    def profiles(self):
        r = self.get('user/%s/aws/profiles' % self.user['_id'])
        return r

    @property
    def profile(self):
        if not hasattr(self, '_profile'):
            self._profile = self.remote_profile()

        return self._profile

    @profile.setter
    def profile(self, profile):
        rp = self.remote_profile(profile['name'])
        if rp is not None:
            logging.debug('Using pre-existing profile: %s (%s)' %
                          (rp['name'], rp['_id']))
            self._profile = rp

            self.wait_for_status(
                'user/%s/aws/profiles/%s/status' % (
                    self.user['_id'], self.profile['_id']),
                'available')
            return None

        # Profile could not be found, create it
        r = self.post('user/%s/aws/profiles' % self.user['_id'],
                      data=json.dumps(profile))

        logging.debug('Posted profile %s: %s' % (r['_id'], r))
        self._profile = r

        self.wait_for_status(
            'user/%s/aws/profiles/%s/status' % (
                self.user['_id'], self.profile['_id']),
            'available')

    @profile.deleter
    def profile(self):
        if self.profile is None:
            logging.debug(
                'No profile with name "%s" found. Skipping.' %
                self.profile_name)

            return None

        self.delete('user/%s/aws/profiles/%s' %
                    (self.user['_id'], self.profile['_id']))
        if hasattr(self, '_profile'):
            del(self._profile)

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
            raise RuntimeError('No profile section found!')

    def remote_volume(self, name=None):
        if name is None:
            name = self.volume_name
        for v in self.volumes:
            if v['name'] == name:
                return v
        return None

    @property
    def volumes(self):
        r = self.get('volumes')
        return r

    @property
    def volume(self):
        if not hasattr(self, '_volume'):
            self._volume = self.remote_volume()

        return self._volume

    @volume.setter
    def volume(self, volume):
        rv = self.remote_volume(volume['name'])
        if rv is not None:
            logging.debug('Using pre-existing volume: %s (%s)' %
                          (rv['name'], rv['_id']))
            self._volume = rv
            return None

        # Volume could not be found, create it
        r = self.post('volumes', data=json.dumps(volume))

        self._volume = r
        logging.debug('Created volume %s: %s' % (r['_id'], r))

        self.wait_for_status(
            'volumes/%s/status' % (self.volume['_id']),
            'created')
        return None

    @volume.deleter
    def volume(self):
        if self.volume is None:
            logging.debug(
                'No Volume with name "%s" found. Skipping' % self.volume_name)
            return None

        _id = self.volume['_id']

        timeout = 300
        self.delete('volumes/%s' % _id)

        if hasattr(self, '_volume'):
            del(self._volume)

        start = time.time()
        while True:
            try:
                self.client.get('volumes/%s/status' % _id)
            except girder_client.HttpError as e:
                if e.status == 400:
                    return None
                else:
                    raise e

            if time.time() - start > timeout:
                raise RuntimeError(
                    'Volume at "%s" never deleted' % _id)
            time.sleep(1)

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
            raise RuntimeError('No profile section found!')

    def attach_volume(self, cluster, volume, path='/mnt/data'):
        self.put('volumes/%s/clusters/%s/attach' %
                 (volume['_id'], cluster['_id']),
                 data=json.dumps({'path': path}))

        log_url = 'volumes/%s/log' % volume['_id']

        self.wait_for_status(
            'volumes/%s/status' % volume['_id'],
            'in-use',
            log_url=log_url, timeout=600)

    def detach_volume(self, volume):
        self.put('volumes/%s/detach' % volume['_id'])

        log_url = 'volumes/%s/log' % volume['_id']

        self.wait_for_status(
            'volumes/%s/status' % volume['_id'],
            'available',
            log_url=log_url, timeout=600)

    def remote_cluster(self, name=None):
        if name is None:
            name = self.cluster_name

        for c in self.clusters:
            if c['name'] == name:
                return c
        return None

    @property
    def clusters(self):
        r = self.get('clusters')
        return r

    @property
    def cluster(self):
        if not hasattr(self, '_cluster'):
            self._cluster = self.remote_cluster()

        return self._cluster

    @cluster.setter
    def cluster(self, cluster):
        rc = self.remote_cluster(cluster['name'])
        if rc is not None:
            logging.debug('Using pre-existing cluster: %s (%s)' %
                          (rc['name'], rc['_id']))
            self._cluster = rc
            return None

        # Profile could not be found, create it
        r = self.post('clusters', data=json.dumps(cluster))
        logging.debug('Posted cluster %s: %s' % (r['_id'], r))

        self._cluster = r

        return None

    @cluster.deleter
    def cluster(self):
        if self.cluster is None:
            logging.debug(
                'No profile with name "%s" found. Skipping' %
                self.profile_name)
            return None

        self.delete('clusters/%s' % self.cluster['_id'])
        if hasattr(self, '_cluster'):
            del(self._cluster)

        return None

    def get_traditional_cluster_body(self):
        if self.cluster_section:
            return {
                'config': {
                    'ssh': {
                        'user': self.cluster_user
                    },
                    'host': self.cluster_host,
                    'port': self.cluster_port
                },
                'name': self.cluster_name,
                'type': 'trad'
            }
        else:
            raise RuntimeError('No cluster section found!')

    def get_ansible_cluster_body(self):
        if self.cluster_section:
            return {
                'config': {
                    'launch': {
                        'spec': 'ec2',
                        'params': {
                            'master_instance_type':
                            self.cluster_master_instance_type,
                            'master_instance_ami':
                            self.cluster_master_instance_ami,
                            'node_instance_count':
                            self.cluster_node_instance_count,
                            'node_instance_type':
                            self.cluster_node_instance_type,
                            'node_instance_ami':
                            self.cluster_node_instance_ami,
                            'terminate_wait_timeout':
                            int(self.cluster_terminate_wait_timeout)
                        }
                    }
                },
                'profileId': self.profile['_id'],
                'name': self.cluster_name,
                'type': self.cluster_type
            }
        else:
            raise RuntimeError('No cluster section found!')

    def get_cluster_body(self):
        if self.cluster_type == 'trad':
            return self.get_traditional_cluster_body()
        else:
            return self.get_ansible_cluster_body()

    def check_log(self, log_url, log_offset):
        r = self.get(log_url, parameters={
            'offset': log_offset
        })

        log_offset = len(r['log'])

        for entry in r['log']:
            logging.debug('%s' % entry)
            try:
                if entry['type'] == 'task':
                    if entry['status'] in ['finished',
                                           'skipped', 'starting']:
                        logging.info('ANSIBLE - %s (%s)' %
                                     (entry['msg'],
                                      entry['status']))
                    elif entry['status'] == 'error':
                        logging.error('ANSIBLE - %s (%s)' %
                                      (entry['msg'],
                                       entry['status']))
            except KeyError:
                logging.log(entry['levelno'], '%s' % entry['msg'])

        return log_offset

    def wait_for_status(self, status_url, status, timeout=10,
                        log_url=None, callback=None):
        logging.debug('Waiting for status "%s" at "%s"' % (status, status_url))

        if isinstance(status, six.string_types):
            status = (status, )

        if log_url is not None:
            r = self.get(log_url)
            log_offset = len(r['log'])
            logging.debug('Log offset set to: %s' % log_offset)

        start = time.time()
        while True:
            if log_url is not None:
                try:
                    log_offset += self.check_log(log_url, log_offset)
                except girder_client.HttpError as ex:
                    logging.debug('Got %s when requesting %s' %
                                  (ex.status, log_url))

            r = self.get(status_url)

            if r['status'] in status:
                break

            if r['status'] in ['error', 'unexpectederror']:
                if log_url is not None:
                    raise RuntimeError(
                        '%s has moved into an error state! \n'
                        'See: %s/%s for more information' %
                        (status_url, self.girder_api_url, log_url))
                else:
                    raise RuntimeError(
                        'Operation moved resource into an error state!'
                        ' See: %s/%s' % (self.girder_api_url, status_url))

            if hasattr(callback, '__call__'):
                if callback(r):
                    break

            if time.time() - start > timeout:
                raise RuntimeError(
                    'Resource at "%s" never moved into the "%s" state, current'
                    ' state is "%s"' % (status_url, status, r['status']))

            time.sleep(1)

    def launch_cluster(self, cluster, timeout=300):
        status_url = 'clusters/%s/status' % cluster['_id']
        log_url = 'clusters/%s/log' % cluster['_id']

        self.put('clusters/%s/launch' % cluster['_id'])

        self.wait_for_status(status_url, 'running',
                             timeout=timeout,
                             log_url=log_url)

    def terminate_cluster(self, cluster, timeout=300):
        if 'status' in cluster and cluster['status'] == 'terminated':
            return None

        status_url = 'clusters/%s/status' % cluster['_id']
        log_url = 'clusters/%s/log' % cluster['_id']

        self.put('clusters/%s/terminate' % cluster['_id'])

        self.wait_for_status(status_url, 'terminated',
                             timeout=timeout,
                             log_url=log_url)

    @property
    def ec2(self):
        import boto3
        return boto3.resource(
            'ec2', aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_regionName)

    def get_instance(self, _id):
        return self.ec2.Instance(_id)

    def get_instances(self):
        for instance in self.ec2.instances.all():
            yield instance

    def get_volumes(self):
        for volume in self.ec2.volumes.all():
            yield volume

    def get(self, uri, **kwargs):
        url = '%s/%s' % (self.girder_api_url, uri)
        logging.debug('GET %s' % url)
        if kwargs:
            logging.debug(kwargs)
        try:
            return self.client.get(uri, **kwargs)
        except girder_client.HttpError as e:
            error = json.loads(e.responseText)
            logging.debug(error)
            try:
                raise RuntimeError(error['message'])
            except KeyError:
                raise e

    def post(self, uri, **kwargs):
        url = '%s/%s' % (self.girder_api_url, uri)
        logging.debug('POST %s' % url)
        if kwargs:
            logging.debug(kwargs)
        try:
            return self.client.post(uri, **kwargs)
        except girder_client.HttpError as e:
            error = json.loads(e.responseText)
            try:
                raise RuntimeError(error['message'])
            except KeyError:
                raise e

    def put(self, uri, **kwargs):
        url = '%s/%s' % (self.girder_api_url, uri)
        logging.debug('PUT %s' % url)
        if kwargs:
            logging.debug(kwargs)
        try:
            return self.client.put(uri, **kwargs)
        except girder_client.HttpError as e:
            error = json.loads(e.responseText)
            logging.debug(error)
            try:
                raise RuntimeError(error['message'])
            except KeyError:
                raise e

    def delete(self, uri, **kwargs):
        url = '%s/%s' % (self.girder_api_url, uri)
        logging.debug('DELETE %s' % url)
        if kwargs:
            logging.debug(kwargs)
        try:
            return self.client.delete(uri, **kwargs)
        except girder_client.HttpError as e:
            error = json.loads(e.responseText)
            logging.debug(error)
            try:
                raise RuntimeError(error['message'])
            except KeyError:
                raise e
