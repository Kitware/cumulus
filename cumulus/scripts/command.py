import click
from utils import logging, Proxy, CONFIG_PARAM
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
    logging.info("Finished creating profile")


@cli.command()
@pass_proxy
def list_profiles(proxy):
    logging.info("Listing profiles")

    keys    = ['name', 'status', '_id', 'regionName', 'cloudProvider']
    headers = ['Name', 'Status', 'ID',  'Region',     'Cloud Provider']

    print tabulate([[p[k] for k in keys] for p in proxy.profiles],
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
    logging.info("Finished creting cluster")


@cli.command()
@click.option('--profile_section', default='profile')
@pass_proxy
def list_clusters(proxy, profile_section):
    logging.info("Listing clusters")
    proxy.profile_section = profile_section

    keys    = ['name', 'status', '_id', 'profileId' ]
    headers = ['Name', 'Status', 'ID',  'Profile ID']


    print tabulate([[c[k] for k in keys] for c in proxy.clusters],
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
    del proxy.cluster
    logging.info("Finished deleting cluster")

@cli.command()
@click.option('--profile_section', default='profile')
@pass_proxy
def delete_profile(proxy, profile_section):
    logging.info("Deleting profile")
    proxy.profile_section = profile_section
    del proxy.profile
    logging.info("Finished deleting profile")

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
def list_instances(proxy):
    logging.info("Listing instances")

    def attr(name):
        def _attr(instance):
            return getattr(instance, name) if hasattr(instance, name) else ''
        return _attr

    def state(instance):
        try:
            return instance.state['Name']
        except Exception:
            return "UNKNOWN"

    def name(instance):
        for tag in instance.tags:
            if tag['Key'] == 'Name':
                return tag['Value']
        return ''

    def groups(instance):
        return ','.join([g['GroupId'] for g in instance.security_groups])

    keys = [name, attr('instance_id'), attr('instance_type'),
            state, attr('public_ip_address'), attr('private_ip_address'),
            attr('key_name'), groups]
    headers = ['Name', 'ID', 'Type', 'State', 'Public IP', 'Private IP', 'Key Name', "Security Groups"]

    print tabulate([[f(i) for f in keys] for i in proxy.get_instances()],
                   headers=headers)

    print "\n"

    logging.info("Finished listing instances")

if __name__ == "__main__":
    cli()
