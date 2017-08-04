from __future__ import print_function
import click
from utils import logging, Proxy, CONFIG_PARAM
from utils import (key,
                   attr,
                   get_profile,
                   aws_name_from_tag)
from tabulate import tabulate

pass_proxy = click.make_pass_decorator(Proxy)

PROFILE_SECTION_HELP = \
    'Pull profile vars from this section (default: "profile").'
CLUSTER_SECTION_HELP = \
    'Pull cluster vars from this section of the config (default: "cluster")'
VOLUME_SECTION_HELP = \
    'Pull volume vars from this section of the config (default: "volume")'


@click.group()
@click.option('-v', '--verbose', count=True,
              help='Print logging information.')
@click.option('--config', default='integration.cfg', type=CONFIG_PARAM,
              help='Speficy the config file path')
@click.option('--girder_section', default='girder',
              help='Section of the config that specifies girder variables')
@click.option('--aws_section', default='aws',
              help='Section of the config that specifies AWS variables')
@click.pass_context
def cli(ctx, verbose, config, girder_section, aws_section):

    if verbose == 1:
        logging.basicConfig(
            format='%(asctime)s %(levelname)-5s - %(message)s',
            level=logging.INFO)
    elif verbose > 1:
        logging.basicConfig(
            format='%(asctime)s %(levelname)-5s - %(message)s',
            level=logging.DEBUG)

    ctx.obj = Proxy(
        config,
        girder_section=girder_section,
        aws_section=aws_section)
    ctx.obj.verbose = verbose


###############################################################################
#   Profile Commands
#

@cli.group()
@click.option('--profile_section', default='profile',
              help=PROFILE_SECTION_HELP)
@pass_proxy
def profile(proxy, profile_section):
    """Subcommand for creating, listing, and deleting profiles"""
    proxy.profile_section = profile_section


@profile.command(name='create')
@pass_proxy
def create_profile(proxy):
    """Idempotently create the profile defined in [profile_section].

    Section of the configuration is 'profile' by default.  Use the
    profile_section option on the profile subcommand to specifiy a
    different section from the config. E.g.:

    $> cumulus profile --profile_section 'other_section' create
    """
    logging.info('Creating profile "%s"' % proxy.profile_name)
    proxy.profile = proxy.get_profile_body()
    logging.info('Finished creating profile "%s" (%s)' %
                 (proxy.profile_name, proxy.profile['_id']))


@profile.command(name='list')
@pass_proxy
def list_profiles(proxy):
    """List the profiles that currently exsit in girder."""

    print('Listing profiles:')

    keys = [key('name'), key('status'), key('_id'),
            key('regionName'), key('cloudProvider')]
    headers = ['Name', 'Status', 'Profile ID', 'Region', 'Cloud Provider']

    print(tabulate([[f(p) for f in keys] for p in proxy.profiles],
                   headers=headers))
    print('\n')


@profile.command(name='delete')
@pass_proxy
def delete_profile(proxy):
    """Idempotently delete the profile defined in [profile_section].

    Section of the configuration is 'profile' by default.  Use the
    profile_section option on the profile subcommand to specifiy a
    different section from the config. E.g.:

    $> cumulus profile --profile_section 'other_section' delete
    """

    try:
        _id = proxy.profile['_id']
        logging.info('Deleting profile "%s"' % proxy.profile_name)
        try:
            del proxy.profile
            logging.info('Finished deleting profile "%s" (%s)' %
                         (proxy.profile_name, _id))
        except RuntimeError as e:
            logging.error(e.message)

    except TypeError:
        logging.info('Found no profile "%s", Skipping.' % proxy.profile_name)

###############################################################################
#   Cluster Commands
#


@cli.group()
@click.option('--profile_section', default='profile',
              help=PROFILE_SECTION_HELP)
@click.option('--cluster_section', default='cluster',
              help=CLUSTER_SECTION_HELP)
@pass_proxy
def cluster(proxy, profile_section, cluster_section):
    """Subcommand for creating, listing, launching, terminating and deleting
    clusters."""
    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section


@cluster.command(name='create')
@pass_proxy
@click.pass_context
def create_cluster(ctx, proxy):
    r"""Idempotently create the cluster defined in [cluster_section].

    Section of the configuration is 'cluster' by default.  Use the
    cluster_section option on the profile subcommand to specifiy a
    different section from the config. E.g.:

    $> cumulus cluster --cluster_section 'other_section' create

    Clusters by default will create the profiles they need based on
    the profile_section passed to the cluster sub-command. E.g.:

    $> cumulus cluster --profile_section 'other_profile' \\
                       --cluster_section 'other_cluster' create

    This will create a profile from the [other_profile] section
    of the config,  and a cluster from the [other_cluster] section.
    By default running:

    $> cumulus create cluster

    will idempotently create a profile from the [profile] section and
    a cluster from the [cluster] section.
    """

    logging.info('Creating cluster "%s"' % proxy.cluster_name)

    if proxy.profile is None:
        ctx.invoke(create_profile)

    proxy.cluster = proxy.get_cluster_body()

    logging.info('Finished creating cluster "%s" (%s)' %
                 (proxy.cluster_name, proxy.cluster['_id']))


@cluster.command(name='list')
@pass_proxy
def list_clusters(proxy):
    """List the clusters that currently exist in girder."""
    print('Listing clusters:')

    keys = [key('name'), key('status'), key('_id'),
            get_profile(proxy.profiles)]
    headers = ['Name', 'Status', 'Cluster ID', 'Profile']

    print(tabulate([[f(c) for f in keys] for c in proxy.clusters],
                   headers=headers))
    print('\n')


@cluster.command(name='delete')
@pass_proxy
def delete_cluster(proxy):
    """Idempotently delete the cluster defined in [cluster_section].

    Section of the configuration is 'cluster' by default.  Use the
    cluster_section option on the cluster subcommand to specifiy a
    different section from the config. E.g.:

    $> cumulus cluster --cluster_section 'other_section' delete

    """
    try:
        _id = proxy.cluster['_id']
        logging.info('Deleting cluster "%s"' % proxy.cluster_name)
        try:
            del proxy.cluster
            logging.info('Finished deleting cluster "%s" (%s)' %
                         (proxy.cluster_name, _id))
        except RuntimeError as e:
            logging.error(e.message)

    except TypeError:
        logging.info('No cluster "%s" found. Skipping' % proxy.cluster_name)


@cluster.command(name='launch')
@pass_proxy
def launch_cluster(proxy):
    """Launch the cluster defined in [cluster_section].

    This will launch the cluster defeind in cluster_section on
    AWS. The cluster must exist in girder before it can be launched.
    """
    if proxy.cluster is None:
        logging.error(
            'No cluster "%s" found. Must create cluster befor launching.' %
            proxy.cluster_name)
    else:
        logging.info('Launching cluster "%s"' % proxy.cluster_name)
        try:
            proxy.launch_cluster(proxy.cluster)
            logging.info('Finished launching cluster %s  (%s)' %
                         (proxy.cluster_name, proxy.cluster['_id']))
        except RuntimeError as e:
            logging.error(e.message)


@cluster.command(name='terminate')
@pass_proxy
def terminate_cluster(proxy):
    """Terminate the cluster defined in [cluster_section].

    This will terminate the cluster defeind in [cluster_section].
    The cluster must exist in girder,  and must have been previously
    launched.
    """
    if proxy.cluster is None:
        logging.info('No cluster "%s" found. Skipping' % proxy.cluster_name)
    elif 'status' in proxy.cluster and \
         proxy.cluster['status'] in ('terminated', 'terminating'):
        logging.info(
            'Cluster "%s" is either terminating or terminated. Skipping' %
            proxy.cluster_name)
    else:
        try:
            logging.info('Terminating cluster "%s"' % proxy.cluster_name)

            proxy.terminate_cluster(proxy.cluster)

            logging.info('Finished terminating cluster "%s" (%s)' %
                         (proxy.cluster_name, proxy.cluster['_id']))
        except RuntimeError as e:
            logging.error(e.message)

    return None

###############################################################################
#   AWS Commands
#


@cli.group()
@pass_proxy
def aws(proxy):
    """Command for displaying information about AWS instances and volumes."""
    pass


@aws.command(name='instances')
@pass_proxy
def list_aws_instances(proxy):
    """Print a list of instances on AWS"""
    print('Listing AWS instances:')

    headers, data = get_aws_instance_info(proxy)

    print(tabulate(data, headers=headers))
    print('\n')

    logging.info('Finished listing AWS instances')


@aws.command(name='volumes')
@pass_proxy
def list_aws_volumes(proxy):
    """Print a list of provisioned EBS volumes on AWS"""
    print('Listing AWS volumes')

    headers, data = get_aws_volume_info(proxy)

    print(tabulate(data, headers=headers))
    print('\n')


def get_aws_instance_info(proxy):
    def state(instance):
        try:
            return instance.state['Name']
        except Exception:
            return 'UNKNOWN'

    def groups(instance):
        return ','.join([g['GroupId'] for g in instance.security_groups])

    keys = [aws_name_from_tag, attr('instance_id'), attr('instance_type'),
            state, attr('public_ip_address'), attr('private_ip_address'),
            attr('key_name'), groups]
    headers = ['Name', 'ID', 'Type', 'State', 'Public IP', 'Private IP',
               'Key Name', 'Security Groups']

    return headers, [[f(i) for f in keys] for i in proxy.get_instances()]


def get_aws_volume_info(proxy):
    def instance_id(volume):
        for a in volume.attachments:
            if a['State'] == 'attached':
                return a['InstanceId']
        return ''

    def instance_name(instances):
        instance_dict = {i.id: aws_name_from_tag(i) for i in instances}

        def _instance_name(volume):
            _id = instance_id(volume)
            if _id != '':
                return instance_dict[_id]

            return ''

        return _instance_name

    def device(volume):
        for a in volume.attachments:
            if a['State'] == 'attached':
                return a['Device']
        return ''

    keys = [
        aws_name_from_tag,
        attr('id'),
        attr('state'),
        attr('size'),
        attr('availability_zone'),
        instance_name(proxy.get_instances()),
        instance_id,
        device]

    headers = ['Name', 'Volume ID', 'State', 'Size', 'Zone',
               'Instance Name', 'Instance ID', 'Device']

    return headers, [[f(v) for f in keys] for v in proxy.get_volumes()]

###############################################################################
#   Volume Commands
#


@cli.group()
@click.option('--profile_section', default='profile',
              help=PROFILE_SECTION_HELP)
@click.option('--cluster_section', default='cluster',
              help=CLUSTER_SECTION_HELP)
@click.option('--volume_section', default='volume',
              help=VOLUME_SECTION_HELP)
@pass_proxy
def volume(proxy, profile_section, cluster_section, volume_section):
    """Subcommand for creating, listing, attaching, detaching and deleting
    clusters."""
    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section
    proxy.volume_section = volume_section


@volume.command(name='create')
@pass_proxy
def create_volume(proxy):
    """Create a volume in girder from the [volume_section].

    This command creates a volume in girder. Note that volumes are
    not initially created on AWS until they are attached to a cluster.

    Volume information is read from the configuration file under
    the config section defeind by the --volume_section flag (default
    'volume').  Pass the --volume_section flag to the volume subcommand
    to switch sections. E.g.:

    $> cumulus volume --volume_section='other_volume' create

    The volume subcommand also accepts --profile_section and
    --cluster_section configuration options for specifying which config
    sections to use for each of those components.
    """
    logging.info('Creating volume')
    proxy.volume = proxy.get_volume_body()
    logging.info('Finished creating volume %s' % proxy.volume['_id'])


@volume.command(name='list')
@pass_proxy
def list_volumes(proxy):
    """List the volumes currently defined in girder."""
    print('Listing volumes:')

    keys = [key('name'), get_profile(proxy.profiles), key('_id'),
            key('size'), key('status'), key('type'), key('zone')]

    headers = ['Name', 'Profile', 'Volume ID',
               'Size', 'Status', 'Type', 'Zone']

    print(tabulate([[f(v) for f in keys] for v in proxy.volumes],
                   headers=headers))
    print('\n')


@volume.command(name='attach')
@pass_proxy
def attach_volume(proxy):
    """Attach volume defined in [volume_section] to cluster
    defined in [cluster_section].

    Volume must first exist in girder (e.g. created via cumulus
    volume create).
    """

    if proxy.cluster is None:
        logging.info('No cluster "%s" found. Skipping' % proxy.cluster_name)
        return None
    elif proxy.volume is None:
        logging.info('No volume "%s" found. Skipping' % proxy.volume_name)
        return None
    else:
        # Check we have a running cluster
        if 'status' in proxy.cluster and proxy.cluster['status'] != 'running':
            logging.error(
                'Can only attach volume to a running cluster '
                '(current state: %s).' % proxy.cluster['status'])
            return None

        logging.info('Attaching volume "%s" (%s) to cluster "%s" (%s)' %
                     (proxy.volume_name, proxy.volume['_id'],
                      proxy.cluster_name, proxy.cluster['_id']))
        try:
            proxy.attach_volume(proxy.cluster, proxy.volume)
            logging.info('Finished attaching volume "%s" (%s).' %
                         (proxy.volume_name, proxy.volume['_id']))
        except RuntimeError as e:
            logging.error(e.message)
        return None


@volume.command(name='detach')
@pass_proxy
def detach_volume(proxy):
    """Detach volume defined in [volume_section] from cluster
    defined in [cluster_section]

    Volume must be attached to a cluster for detach to succeed.
    """

    if proxy.volume is None:
        logging.info('No volume "%s" found. Skipping' % proxy.volume_name)
        return None
    elif 'status' in proxy.volume and \
         proxy.volume['status'] != 'in-use':
        logging.error('Cannot detach volume "%s", in state "%s"' %
                      (proxy.volume_name, proxy.volume['status']))
        return None

    else:
        try:
            logging.info('Detaching volume %s' % proxy.volume['_id'])

            proxy.detach_volume(proxy.volume)

            logging.info('Finished detaching volume "%s" (%s).' %
                         (proxy.volume_name, proxy.volume['_id']))
        except RuntimeError as e:
            logging.error(e.message)

    return None


@volume.command(name='delete')
@pass_proxy
def delete_volume(proxy):
    """Delete volume defined in [volume_section] to cluster
    defined in [cluster_section].

    Volume must be in state 'available' (i.e. not attached to any
    cluster) for delete to succeed.
    """
    try:
        _id = proxy.volume['_id']
        logging.info('Deleting volume "%s"' % proxy.volume_name)
        try:
            del proxy.volume
            logging.info('Finished deleting volume "%s" (%s)' %
                         (proxy.volume_name, _id))
        except RuntimeError as e:
            logging.error(e.message)
    except TypeError:
        logging.info('No volume "%s" found. skipping' % proxy.cluster_name)


###############################################################################
#   Status Commands
#


@cli.command()
@click.pass_context
def full_status(ctx):
    """Print the full status of system.

    This includes printing girder's profiles, clusters, and volumes
    as well as all AWS instances and AWS volumes."""
    print('************************* Full Status ****************************')
    ctx.invoke(list_profiles)
    ctx.invoke(list_clusters)
    ctx.invoke(list_volumes)
    ctx.invoke(list_aws_instances)
    ctx.invoke(list_aws_volumes)
    print('************************** End  Status ***************************')


if __name__ == '__main__':
    cli()
