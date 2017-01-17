import click
from utils import logging, Proxy, CONFIG_PARAM
from utils import (key,
                   attr,
                   get_profile,
                   aws_name_from_tag)
from tabulate import tabulate

pass_proxy = click.make_pass_decorator(Proxy)


@click.group()
@click.option('-v', '--verbose', count=True)
@click.option('--config', default='integration.cfg', type=CONFIG_PARAM)
@click.option('--girder_section', default='girder')
@click.option('--aws_section', default='aws')
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
@click.option('--profile_section', default='profile')
@pass_proxy
def profile(proxy, profile_section):
    proxy.profile_section = profile_section


@profile.command(name='create')
@pass_proxy
def create_profile(proxy):
    logging.info('Creating profile "%s"' % proxy.profile_name)
    proxy.profile = proxy.get_profile_body()
    logging.info('Finished creating profile "%s" (%s)' %
                 (proxy.profile_name, proxy.profile['_id']))


@profile.command(name='list')
@pass_proxy
def list_profiles(proxy):
    print('Listing profiles:')

    keys = [key('name'), key('status'), key('_id'),
            key('regionName'), key('cloudProvider')]
    headers = ['Name', 'Status', 'Profile ID',  'Region', 'Cloud Provider']

    print tabulate([[f(p) for f in keys] for p in proxy.profiles],
                   headers=headers)
    print '\n'


@profile.command(name='delete')
@pass_proxy
def delete_profile(proxy):
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
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@pass_proxy
def cluster(proxy, profile_section, cluster_section):
    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section


@cluster.command(name='create')
@pass_proxy
@click.pass_context
def create_cluster(ctx, proxy):
    logging.info('Createing cluster "%s"' % proxy.cluster_name)

    if proxy.profile is None:
        ctx.invoke(create_profile)

    proxy.cluster = proxy.get_cluster_body()

    logging.info('Finished creting cluster "%s" (%s)' %
                 (proxy.cluster_name, proxy.cluster['_id']))


@cluster.command(name='list')
@pass_proxy
def list_clusters(proxy):
    print('Listing clusters:')

    keys = [key('name'), key('status'), key('_id'),
            get_profile(proxy.profiles)]
    headers = ['Name', 'Status', 'Cluster ID',  'Profile']

    print tabulate([[f(c) for f in keys] for c in proxy.clusters],
                   headers=headers)
    print '\n'


@cluster.command(name='delete')
@pass_proxy
def delete_cluster(proxy):
    # import pudb; pu.db
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
    pass


@aws.command(name='instances')
@pass_proxy
def list_aws_instances(proxy):
    print('Listing AWS instances:')

    headers, data = get_aws_instance_info(proxy)

    print tabulate(data, headers=headers)
    print '\n'

    logging.info('Finished listing AWS instances')


@aws.command(name='volumes')
@pass_proxy
def list_aws_volumes(proxy):
    print('Listing AWS volumes')

    headers, data = get_aws_volume_info(proxy)

    print tabulate(data, headers=headers)
    print '\n'


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
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@click.option('--volume_section', default='volume')
@pass_proxy
def volume(proxy, profile_section, cluster_section, volume_section):
    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section
    proxy.volume_section = volume_section


@volume.command(name='create')
@pass_proxy
def create_volume(proxy):
    logging.info('Creating volume')
    proxy.volume = proxy.get_volume_body()
    logging.info('Finished creating volume %s' % proxy.volume['_id'])


@volume.command(name='list')
@pass_proxy
def list_volumes(proxy):
    print('Listing volumes:')

    keys = [key('name'), get_profile(proxy.profiles), key('_id'),
            key('size'), key('status'), key('type'), key('zone')]

    headers = ['Name', 'Profile', 'Volume ID',
               'Size', 'Status', 'Type', 'Zone']

    print tabulate([[f(v) for f in keys] for v in proxy.volumes],
                   headers=headers)
    print '\n'


@volume.command(name='attach')
@pass_proxy
def attach_volume(proxy):
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
    print('************************* Full Status ****************************')
    ctx.invoke(list_profiles)
    ctx.invoke(list_clusters)
    ctx.invoke(list_volumes)
    ctx.invoke(list_aws_instances)
    ctx.invoke(list_aws_volumes)
    print('************************** End  Status ***************************')


if __name__ == '__main__':
    cli()
