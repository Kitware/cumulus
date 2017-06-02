from __future__ import print_function
import os
import click
import json
import logging
import functools
import sys
import time
import girder_client
import paramiko
from contextlib import contextmanager

from cumulus.scripts.command import (cli, pass_proxy,
                                     get_aws_instance_info,
                                     get_aws_volume_info)

from cumulus.scripts.command import (profile,
                                     cluster,
                                     create_profile,
                                     create_cluster,
                                     launch_cluster,
                                     create_volume,
                                     attach_volume,
                                     detach_volume,
                                     delete_volume,
                                     terminate_cluster,
                                     delete_cluster,
                                     delete_profile)

test_failures = []

def report():
    if len(test_failures) == 0:
        print('\nAll tests passed')
        sys.exit(0)
    else:
        print('\nTest failures present!')
        for test in test_failures:
            print('    {}'.format(test))
        sys.exit(1)


def test_case(func):
    @functools.wraps(func)
    def _catch_exceptions(*args, **kwargs):
        try:
            ctx, proxy = args[0], args[1]
            if proxy.verbose >= 1:
                print('%s...' % func.__name__)
                sys.stdout.flush()

            func(*args, **kwargs)

            if proxy.verbose >= 1:
                print('%s...OK' % func.__name__)
            else:
                print('.', end='')

        except AssertionError as e:
            test_failures.append(func.__name__)
            if proxy.verbose >= 1:
                import traceback
                print('ERROR')
                traceback.print_exc()
            else:
                print('E', end='')

        sys.stdout.flush()

    _catch_exceptions = click.pass_context(_catch_exceptions)
    _catch_exceptions = pass_proxy(_catch_exceptions)

    return _catch_exceptions


@contextmanager
def clean_proxy(proxy):
    if hasattr(proxy, '_volume'):
        del(proxy._volume)
    if hasattr(proxy, '_cluster'):
        del(proxy._cluster)
    if hasattr(proxy, '_profile'):
        del(proxy._profile)
    yield


def invoke_with_clean_proxy(ctx, proxy):
    def _invoke_with_clean_proxy(func, **kwargs):
        with clean_proxy(proxy):
            ctx.invoke(func, **kwargs)
    return _invoke_with_clean_proxy


@cli.command()
@click.option('--profile_section', default='profile')
@test_case
def test_profile(ctx, proxy, profile_section):
    """Test profile creation/deletion."""
    proxy.profile_section = profile_section
    num_profiles = len(proxy.profiles)
    invoke = invoke_with_clean_proxy(ctx, proxy)

    invoke(create_profile)

    assert len(proxy.profiles) == num_profiles + 1, \
        'After create_profile only one profile should exist'

    invoke(delete_profile)

    assert len(proxy.profiles) == num_profiles, \
        'After delete_profile no profiles should exist'


@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@test_case
def test_cluster(ctx, proxy, profile_section, cluster_section):
    """Test cluster creation/deletion."""
    num_clusters = len(proxy.clusters)

    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section
    invoke = invoke_with_clean_proxy(ctx, proxy)

    invoke(create_profile)
    invoke(create_cluster)

    assert len(proxy.clusters) == num_clusters + 1, \
        'After create_cluster only one profile should exist'

    invoke(delete_cluster)
    invoke(delete_profile)

    assert len(proxy.clusters) == num_clusters, \
        'After delete_cluster no profiles should exist'


def get_instance_hash(proxy):
    headers, data = get_aws_instance_info(proxy)
    return {d['ID']: d for d in [dict(zip(headers, i)) for i in data]}

@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@test_case
def test_launch_cluster(ctx, proxy, profile_section, cluster_section):
    """Test launching/terminating a cluster."""
    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section
    invoke = invoke_with_clean_proxy(ctx, proxy)

    begin = get_instance_hash(proxy)

    invoke(create_profile)
    invoke(create_cluster)
    invoke(launch_cluster)

    middle = get_instance_hash(proxy)

    instance_ids = set(middle.keys()) - set(begin.keys())

    assert len(instance_ids) == 2, \
        'Two instances should have been created'

    for instance in [middle[i] for i in instance_ids]:
        assert instance['State'] == 'running', \
            'Instance {} is not running'.format(instance['ID'])
        assert instance['Type'] == 't2.nano', \
            'Instance {} is not a t2.nano instance'.format(instance['ID'])

    invoke(terminate_cluster)
    invoke(delete_cluster)
    invoke(delete_profile)

    end = get_instance_hash(proxy)

    for instance in [end[i] for i in instance_ids]:
        assert instance['State'] == 'terminated', \
            'Instance {} is not running'.format(instance['ID'])


def get_volume_hash(proxy):
    headers, data = get_aws_volume_info(proxy)
    return {d['Volume ID']: d for d in [dict(zip(headers, i)) for i in data]}


@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='cluster')
@click.option('--volume_section', default='volume')
@test_case
def test_volume(ctx, proxy, profile_section, cluster_section, volume_section):
    """Test attaching/detaching/deleting a volume"""
    proxy.profile_section = profile_section
    proxy.cluster_section = cluster_section
    proxy.volume_section = volume_section

    invoke = invoke_with_clean_proxy(ctx, proxy)

    invoke(create_profile)
    invoke(create_cluster)

    invoke(launch_cluster)

    begin = get_volume_hash(proxy)

    invoke(create_volume)

    invoke(attach_volume)

    after_attach = get_volume_hash(proxy)

    vol_ids = set(after_attach.keys()) - set(begin.keys())
    assert len(vol_ids) == 1, \
        'Should have found only one volume'

    vol_id = vol_ids.pop()

    assert after_attach[vol_id]['State'] == 'in-use'
    assert after_attach[vol_id]['Size'] == 12

    girder_vol = proxy.volumes[0]
    girder_cluster = proxy.clusters[0]

    # local girder status is 'in-use' (attached)
    assert girder_vol['status'] == 'in-use'

    # volume has right aws id
    assert vol_id in [v['ec2']['id'] for v in proxy.volumes]

    # cluster has knowledge of girder volume
    assert girder_vol['_id'] in girder_cluster['volumes']

    invoke(detach_volume)

    after_detach = get_volume_hash(proxy)

    # Remote state is 'available'
    assert after_detach[vol_id]['State'] == 'available'

    girder_vol = proxy.volumes[0]
    girder_cluster = proxy.clusters[0]

    # local girder status is available
    assert girder_vol['status'] == 'available'

    # volume has been removed from local girder cluster's volume list
    assert girder_vol['_id'] not in girder_cluster['volumes']

    invoke(delete_volume)

    after = get_volume_hash(proxy)

    # Removed out on AWS
    assert vol_id not in after.keys()

    # Removed locally
    assert len(proxy.volumes) == 0

    invoke(terminate_cluster)

    invoke(delete_cluster)
    invoke(delete_profile)


###############################################################################
#                       Taskflow Integration tests
#


def create_taskflow(proxy, cls_name):
    r = proxy.post('taskflows', data=json.dumps({
        'taskFlowClass': cls_name
    }))

    return r['_id']



def wait_for_taskflow_status(proxy, taskflow_id, state, timeout=60):

    def _get_taskflow_numbers(status_response):
        tasks_url = 'taskflows/%s/tasks' % (taskflow_id)
        try:
            r = proxy.get(tasks_url)
            logging.info('Tasks in flow: %d' % len(r))
        except girder_client.HttpError:
            pass

    proxy.wait_for_status(
        'taskflows/%s/status' % (taskflow_id), state,
        timeout=timeout,
        log_url='taskflows/%s/log' % (taskflow_id),
        callback=_get_taskflow_numbers)



@cli.command()
@test_case
def test_simple_taskflow(ctx, proxy):
    """Test a simple taskflow"""
    logging.info('Running simple taskflow ...')
    taskflow_id = create_taskflow(
        proxy, 'cumulus.taskflow.core.test.mytaskflows.SimpleTaskFlow')

    # Start the task flow
    proxy.put('taskflows/%s/start' % (taskflow_id))
    wait_for_taskflow_status(proxy, taskflow_id, 'complete')


@cli.command()
@test_case
def test_linked_taskflow(ctx, proxy):
    """Test a linked taskflow"""
    logging.info('Running linked taskflow ...')
    taskflow_id = create_taskflow(
        proxy, 'cumulus.taskflow.core.test.mytaskflows.LinkTaskFlow')

    # Start the task flow
    proxy.put('taskflows/%s/start' % (taskflow_id))
    wait_for_taskflow_status(proxy, taskflow_id, 'complete')


@cli.command()
@test_case
def test_terminate_taskflow(ctx, proxy):
    """Test terminating a taskflow"""
    # Test terminating a simple flow
    logging.info('Running simple taskflow ...')
    taskflow_id = create_taskflow(
        proxy, 'cumulus.taskflow.core.test.mytaskflows.SimpleTaskFlow')

    # Start the task flow
    proxy.put('taskflows/%s/start' % (taskflow_id))
    time.sleep(4)

    logging.info('Terminate the taskflow')
    proxy.put('taskflows/%s/terminate' % (taskflow_id))

    # Wait for it to terminate
    wait_for_taskflow_status(proxy, taskflow_id, 'terminated')


    # Now delete it
    logging.info('Delete the taskflow')
    try:
        proxy.delete('taskflows/%s' % (taskflow_id))
    except girder_client.HttpError as ex:
        if ex.status != 202:
            raise

    # Wait for it to delete
    try:
        wait_for_taskflow_status(proxy, taskflow_id, 'deleted')
    except girder_client.HttpError as ex:
        if ex.status != 400:
            raise

@cli.command()
@test_case
def test_chord_taskflow(ctx, proxy):
    """Test running a taskflow with a chord"""
    # Now try something with a chord
    logging.info('Running taskflow containing a chord ...')
    taskflow_id = create_taskflow(
        proxy, 'cumulus.taskflow.core.test.mytaskflows.ChordTaskFlow')

    # Start the task flow
    proxy.put('taskflows/%s/start' % (taskflow_id))
    wait_for_taskflow_status(proxy, taskflow_id, 'complete')


@cli.command()
@test_case
def test_connected_taskflow(ctx, proxy):
    """Test a connected taskflow"""
    # Now try a workflow that is the two connected together
    logging.info('Running taskflow that connects to parts together ...')
    taskflow_id = create_taskflow(
        proxy, 'cumulus.taskflow.core.test.mytaskflows.ConnectTwoTaskFlow')

    # Start the task flow
    proxy.put('taskflows/%s/start' % (taskflow_id))

    # Wait for it to complete
    wait_for_taskflow_status(proxy, taskflow_id, 'complete')


@cli.command()
@test_case
def test_taskflow(ctx, proxy):
    """Run all taskflow tests"""
    ctx.invoke(test_simple_taskflow)
    ctx.invoke(test_linked_taskflow)
    ctx.invoke(test_terminate_taskflow)
    ctx.invoke(test_chord_taskflow)
    ctx.invoke(test_connected_taskflow)


#      # Now try a composite workflow approach ...
#        print ('Running taskflow that is a composite ...')
#        taskflow_id = create_taskflow(
#            client, 'cumulus.taskflow.core.test.mytaskflows.MyCompositeTaskFlow')
#
#        # Start the task flow
#        url = 'taskflows/%s/start' % (taskflow_id)
#        client.put(url)
#
#        # Wait for it to complete
#        wait_for_complete(client, taskflow_id)



###############################################################################
#                  Traditional Cluster Test
#

@cli.command()
@click.option('--profile_section', default='profile')
@click.option('--cluster_section', default='traditional')
@click.option('--host', default=None)
@click.option('--port', default=None)
@test_case
def test_traditional(ctx, proxy, profile_section, cluster_section, host, port):
    """Test creating a traditional cluster"""
    from StringIO import StringIO

    logging.info('Starting traditional cluster test...')
    proxy.cluster_section = cluster_section
    proxy.profile_section = profile_section
    invoke = invoke_with_clean_proxy(ctx, proxy)

    if host is not None:
        proxy.cluster_host = host

    if port is not None:
        proxy.cluster_port = port

    invoke(create_profile)
    invoke(create_cluster)

    proxy.wait_for_status('clusters/%s/status' % proxy.cluster['_id'],
                          'created', timeout=60)

    # Re-request the cluster
    proxy._cluster = None

#    # Check cluster has key location
    assert 'config' in proxy.cluster
    assert 'ssh' in proxy.cluster['config']
    assert 'publicKey' in proxy.cluster['config']['ssh']

    key = proxy.cluster['config']['ssh']['publicKey']

    # Create SSH Client
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.load_system_host_keys()
    client.connect(proxy.cluster_host,
                   username=proxy.cluster_user, look_for_keys=True)

    # Add key on 'cluster' machine
    _, stdout, stderr = client.exec_command('echo "%s" >> ~/.ssh/authorized_keys' % key)
    assert bool(stdout.read()) == False
    assert bool(stderr.read()) == False

    proxy.put('clusters/%s/start' % proxy.cluster['_id'])
    proxy.wait_for_status('clusters/%s/status' % proxy.cluster['_id'],
                          'running', timeout=60)


    # Create Script
    commands = ['sleep 10', 'cat CumulusIntegrationTestInput']
    r = proxy.post('scripts', data=json.dumps({
        'commands': commands,
        'name': 'CumulusIntegrationTestLob'
    }))
    script_id = r['_id']

    # Create Input
    data = 'Need more input!'
    input_folder_id = proxy.get_folder_id('Private/CumulusInput')

    ## Create the input item
    proxy.client.uploadFile(
        input_folder_id, StringIO(data), 'CumulusIntegrationTestInput',
        len(data), parentType='folder')

    # Create Output Folder
    output_folder_id = proxy.get_folder_id('Private/CumulusOutput')

    # Create Job
    job = proxy.client.post('jobs', data=json.dumps({
        'name': 'CumulusIntegrationTestJob',
        'scriptId': script_id,
        'output': [{
            'folderId': output_folder_id,
            'path': '.'
        }],
        'input': [{
            'folderId': input_folder_id,
            'path': '.'
            }]
    }))

    # Submit Job
    proxy.client.put('clusters/%s/job/%s/submit' %
                     (proxy.cluster['_id'], job['_id']))

    proxy.wait_for_status('jobs/%s' % job['_id'],
                          'complete', timeout=60,
                          log_url='jobs/%s/log' % job['_id'])
    # Assert output
    r = proxy.client.listItem(output_folder_id)
    assert len(r) == 4


    # Clean up
    invoke(delete_cluster)
    invoke(delete_profile)




if __name__ == '__main__':
    try:
        cli()
    except SystemExit:
        pass

    report()
