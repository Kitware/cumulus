import click
from utils import logging, Proxy, CONFIG_PARAM
from utils import (key,
                   attr,
                   profile,
                   aws_name_from_tag)
from tabulate import tabulate

pass_proxy = click.make_pass_decorator(Proxy)


@click.group(chain=True)
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


@cli.command()
@click.option('--profile_section', default='profile')
@pass_proxy
def create_profile(proxy, profile_section):
    logging.info("Creating profile")
    proxy.profile_section = profile_section
    proxy.profile = proxy.get_profile_body()
    logging.info("Finished creating profile %s" % proxy.profile['_id'])


@cli.command()
@pass_proxy
def list_profiles(proxy):
    logging.info("Listing profiles")

    keys    = [key('name'), key('status'), key('_id'),
               key('regionName'), key('cloudProvider')]
    headers = ['Name', 'Status', 'Profile ID',  'Region', 'Cloud Provider']

    print tabulate([[f(p) for f in keys] for p in proxy.profiles],
                   headers=headers)
    print "\n"

    logging.info("Finished listing profiles")


@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@pass_proxy
def create_cluster(proxy, profile_section, cluster_section):
    logging.info("Createing cluster")
    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section
    proxy.cluster = proxy.get_cluster_body()
    logging.info("Finished creting cluster %s" % proxy.cluster['_id'])


@cli.command()
@pass_proxy
def list_clusters(proxy):
    logging.info("Listing clusters")


    keys    = [key('name'), key('status'), key('_id'),
               profile(proxy.profiles)]
    headers = ['Name', 'Status', 'Cluster ID',  'Profile']


    print tabulate([[f(c) for f in keys] for c in proxy.clusters],
                   headers=headers)
    print "\n"
    logging.info("Finished listing clusters")

@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@pass_proxy
def delete_cluster(proxy, profile_section, cluster_section):
    logging.info("Deleting cluster")
    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section
    _id = proxy.cluster['_id']
    del proxy.cluster
    logging.info("Finished deleting cluster %s" % _id)

@cli.command()
@click.option('--profile_section', default='profile')
@pass_proxy
def delete_profile(proxy, profile_section):
    logging.info("Deleting profile")
    proxy.profile_section = profile_section
    _id = proxy.profile['_id']
    del proxy.profile
    logging.info("Finished deleting profile %s" % _id)

@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@pass_proxy
def launch_cluster(proxy, profile_section, cluster_section):
    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section

    logging.info("Launching cluster %s" % proxy.cluster['_id'])
    proxy.launch_cluster(proxy.cluster)
    logging.info("Finished launching cluster")

@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@pass_proxy
def terminate_cluster(proxy, profile_section, cluster_section):
    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section

    logging.info("Terminating cluster %s" % proxy.cluster['_id'])
    proxy.terminate_cluster(proxy.cluster)
    logging.info("Finished terminating cluster")


@cli.command()
@pass_proxy
def list_aws_instances(proxy):
    logging.info("Listing AWS instances")

    def state(instance):
        try:
            return instance.state['Name']
        except Exception:
            return "UNKNOWN"


    def groups(instance):
        return ','.join([g['GroupId'] for g in instance.security_groups])

    keys = [aws_name_from_tag, attr('instance_id'), attr('instance_type'),
            state, attr('public_ip_address'), attr('private_ip_address'),
            attr('key_name'), groups]
    headers = ['Name', 'ID', 'Type', 'State', 'Public IP', 'Private IP', 'Key Name', "Security Groups"]

    print tabulate([[f(i) for f in keys] for i in proxy.get_instances()],
                   headers=headers)

    print "\n"

    logging.info("Finished listing AWS instances")


@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--volume_section', default='volume')
@pass_proxy
def create_volume(proxy, profile_section, volume_section):
    logging.info("Creating volume")
    proxy.profile_section = profile_section
    proxy.volume_section = volume_section
    proxy.volume = proxy.get_volume_body()
    logging.info("Finished creating volume %s" % proxy.volume['_id'])


@cli.command()
@pass_proxy
def list_volumes(proxy):
    logging.info("Listing volumes")

    keys    = [key('name'), profile(proxy.profiles), key('_id'),
               key('size'), key('status'), key('type'), key('zone')]

    headers = ['Name', 'Profile', 'Volume ID',  'Size', 'Status', 'Type', 'Zone']

    print tabulate([[f(v) for f in keys] for v in proxy.volumes],
                   headers=headers)
    print "\n"
    logging.info("Finished listing volumes")


@cli.command()
@pass_proxy
def list_aws_volumes(proxy):
    logging.info("Listing AWS volumes")


    def instance_id(volume):
        for a in volume.attachments:
            if a['State'] == 'attached':
                return a['InstanceId']
        return ''

    def instance_name(volume):
        return aws_name_from_tag(proxy.get_instance(instance_id(volume))) \
            if instance_id(volume) != '' else ''

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
        instance_name,
        instance_id,
        device]

    headers = ['Name', 'Volume ID', 'State', 'Size', 'Zone',
               'Instance Name', 'Instance ID', 'Device']

    print tabulate([[f(v) for f in keys] for v in proxy.get_volumes()],
                   headers=headers)
    print "\n"
    logging.info("Finished listing volumes")






if __name__ == "__main__":
    cli()
