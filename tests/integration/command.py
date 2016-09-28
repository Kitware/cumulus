import click
# import os
import ConfigParser
# import requests
from girder_client import GirderClient


class ConfigParam(click.ParamType):
    """Takes a file string and produces a RawConfigParser object"""
    name = 'config'

    def convert(self, value, param, ctx):
        try:
            parser = ConfigParser.RawConfigParser()
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
        url = "http://127.0.0.1:8080"
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
        >>> f.foo_url
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

        self._client = GirderClient(
            apiUrl="%s/api/v1/" % self.girder_url)

        self._client.authenticate(self.girder_user,
                                  self.girder_password)

    def get(self, *args, **kwargs):
        self.client.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        self.client.post(*args, **kwargs)

    def put(self, *args, **kwargs):
        self.client.put(*args, **kwargs)

    def delete(self, *args, **kwargs):
        self.client.delete(*args, **kwargs)


pass_cluster = click.make_pass_decorator(Cluster)


@click.group(chain=True)
@click.option('--config', default='integration.cfg', type=CONFIG_PARAM)
@click.option('--girder_section', default='girder')
@click.option('--aws_section', default='aws')
@click.pass_context
def cli(ctx, config, girder_section, aws_section):
    ctx.obj = Cluster(
        config,
        girder_section=girder_section,
        aws_section=aws_section)


@cli.command()
@click.option('--profile_section', default='profile')
@pass_cluster
def create_profile(cluster, profile_section):
    cluster.profile_section = profile_section
    click.echo(cluster.profile_name)


@cli.command()
@click.option('--profile_section', default='profile')
@pass_cluster
def create_cluster(cluster, profile_section):
    cluster.profile_section = profile_section
    click.echo(cluster.girder_url)

if __name__ == "__main__":
    cli()
