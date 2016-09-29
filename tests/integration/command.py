import click
import ConfigParser
from girder_client import GirderClient
import time

import logging
logging.getLogger('requests').setLevel(logging.CRITICAL)

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


class Cluster(object):

    aws_section = section_property("aws")
    girder_section = section_property("girder")
    profile_section = section_property("profile")
    cluster_section = section_property("cluster")

    def __init__(self, config, aws_section="aws", girder_section='girder'):
        self.config = config

        self.aws_section = aws_section
        self.girder_section = girder_section

        self._client = GirderClient(apiUrl=self.girder_api_url)

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
    def profile(self):
        if not hasattr(self, "_profile"):
            self._profile = None

        if self._profile is None:
            # TODO: actually get or create the profile
            self._profile = {'_id' : 'FIXME'}

        return self._profile

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
                            'terminate_wait_timeout': self.cluster_terminate_wait_timeout
                        }
                    }
                },
                'profileId': self.profile['_id'],
                'name': self.cluster_name,
                'type': self.cluster_type
            }
        else:
            raise RuntimeError("No cluster section found!")

    def wait_for_status(self, status_url, status, timeout=10):

        start = time.time()
        while True:
            time.sleep(1)
            r = self.get(status_url)

            if r['status'] == status:
                break

            if time.time() - start > timeout:
                raise RuntimeError(
                    'Resource never moved into the "%s" state, current '
                    'state is "%s"' % (status, r['status']))


    def get(self, *args, **kwargs):
        return self._client.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        return self._client.post(*args, **kwargs)

    def put(self, *args, **kwargs):
        return self._client.put(*args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._client.delete(*args, **kwargs)


pass_cluster = click.make_pass_decorator(Cluster)


@click.group(chain=True)
@click.option('-v', '--verbose', count=True)
@click.option('--config', default='integration.cfg', type=CONFIG_PARAM)
@click.option('--girder_section', default='girder')
@click.option('--aws_section', default='aws')
@click.pass_context
def cli(ctx, verbose, config, girder_section, aws_section):

    if verbose == 1:
        logging.basicConfig(
            format='%(asctime)s [%(levelname)-5s] - %(message)s',
            level=logging.INFO)
    elif verbose > 1:
        logging.basicConfig(
            format='%(asctime)s [%(levelname)-5s] - %(message)s',
            level=logging.DEBUG)

    ctx.obj = Cluster(
        config,
        girder_section=girder_section,
        aws_section=aws_section)


@cli.command()
@click.option('--profile_section', default='profile')
@pass_cluster
def create_profile(cluster, profile_section):
    cluster.profile_section = profile_section

    logging.info("Creating profile")
    logging.debug('POST %s/user/%s/aws/profiles' % (
        cluster.girder_api_url,
        cluster.user['_id']))
    logging.debug(cluster.get_profile_body())



@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@pass_cluster
def create_cluster(cluster, profile_section, cluster_section):
    cluster.profile_section = profile_section
    cluster.cluster_section = cluster_section

    logging.info("Createing cluster")
    logging.debug('POST %s/clusters' % cluster.girder_api_url)
    logging.debug(cluster.get_cluster_body())

if __name__ == "__main__":
    cli()
