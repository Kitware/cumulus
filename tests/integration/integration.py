from __future__ import print_function
import click
from cumulus.scripts.command import (cli, pass_proxy,
                                     get_aws_instance_info)

from cumulus.scripts.command import (create_profile,
                                     create_cluster,
                                     launch_cluster,
                                     terminate_cluster,
                                     delete_cluster,
                                     delete_profile)
import functools
import sys

test_failures = []

def report():
    if len(test_failures) == 0:
        print("\nAll tests passed")
        sys.exit(0)
    else:
        print("\nTest failures present!")
        for test in test_failures:
            print("    {}".format(test))
        sys.exit(1)


def test_case(func):
    @functools.wraps(func)
    def _catch_exceptions(*args, **kwargs):
        try:
            ctx, proxy = args[0], args[1]
            if proxy.verbose >= 1:
                print('%s...' % func.__name__, end='')

            func(*args, **kwargs)

            if proxy.verbose >= 1:
                print('OK')
            else:
                print('.', end='')

        except AssertionError as e:
            test_failures.append(func.__name__)
            if proxy.verbose >= 1:
                print('ERROR')

                if proxy.verbose >= 2:
                    import traceback
                    traceback.print_exc()
            else:
                print('E', end='')

        sys.stdout.flush()

    _catch_exceptions = click.pass_context(_catch_exceptions)
    _catch_exceptions = pass_proxy(_catch_exceptions)

    return _catch_exceptions


@cli.command()
@click.option('--profile_section', default='profile')
@test_case
def test_profile(ctx, proxy, profile_section):
    assert len(proxy.profiles) == 0, \
        'Profile already exist!'

    ctx.invoke(create_profile, profile_section=profile_section)

    assert len(proxy.profiles) == 1, \
        "After create_profile only one profile should exist"

    ctx.invoke(delete_profile, profile_section=profile_section)

    assert len(proxy.profiles) == 0, \
        "After delete_profile no profiles should exist"


@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@test_case
def test_cluster(ctx, proxy, profile_section, cluster_section):
    assert len(proxy.profiles) == 0, \
        'Profile already exist!'
    assert len(proxy.clusters) == 0, \
        'Clusters already exist!'

    ctx.invoke(create_profile, profile_section=profile_section)
    ctx.invoke(create_cluster, cluster_section=cluster_section)

    assert len(proxy.clusters) == 1, \
        "After create_cluster only one profile should exist"

    ctx.invoke(delete_cluster, cluster_section=cluster_section)
    ctx.invoke(delete_profile, profile_section=profile_section)

    assert len(proxy.clusters) == 0, \
        "After delete_cluster no profiles should exist"


def get_instance_hash(proxy):
    headers, data = get_aws_instance_info(proxy)
    return {d['ID']: d for d in [dict(zip(headers, i)) for i in data]}

@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@test_case
def test_launch_cluster(ctx, proxy, profile_section, cluster_section):
    assert len(proxy.profiles) == 0, \
        'Profile already exist!'
    assert len(proxy.clusters) == 0, \
        'Clusters already exist!'

    begin = get_instance_hash(proxy)

    ctx.invoke(create_profile, profile_section=profile_section)
    ctx.invoke(create_cluster, cluster_section=cluster_section)

    ctx.invoke(launch_cluster, profile_section=profile_section,
               cluster_section=cluster_section)

    middle = get_instance_hash(proxy)

    instance_ids = set(middle.keys()) - set(begin.keys())

    assert len(instance_ids) == 2, \
        "Two instances should have been created"

    for instance in [middle[i] for i in instance_ids]:
        assert instance['State'] == 'running', \
            "Instance {} is not running".format(instance["ID"])
        assert instance['Type'] == 't2.nano', \
            "Instance {} is not a t2.nano instance".format(instance["ID"])

    ctx.invoke(terminate_cluster, profile_section=profile_section,
               cluster_section=cluster_section)

    ctx.invoke(delete_cluster, cluster_section=cluster_section)
    ctx.invoke(delete_profile, profile_section=profile_section)

    end = get_instance_hash(proxy)

    for instance in [end[i] for i in instance_ids]:
        assert instance['State'] == 'terminated', \
            "Instance {} is not running".format(instance["ID"])


if __name__ == '__main__':
    try:
        cli()
    except SystemExit:
        pass

    report()
