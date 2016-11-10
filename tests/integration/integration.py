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
from cumulus.scripts.command import (cli, pass_proxy,
                                     get_aws_instance_info,
                                     get_aws_volume_info)

from cumulus.scripts.command import (create_profile,
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


@cli.command()
@click.option('--profile_section', default='profile')
@test_case
def test_profile(ctx, proxy, profile_section):
    assert len(proxy.profiles) == 0, \
        'Profile already exist!'

    ctx.invoke(create_profile, profile_section=profile_section)

    assert len(proxy.profiles) == 1, \
        'After create_profile only one profile should exist'

    ctx.invoke(delete_profile, profile_section=profile_section)

    assert len(proxy.profiles) == 0, \
        'After delete_profile no profiles should exist'


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
        'After create_cluster only one profile should exist'

    ctx.invoke(delete_cluster, cluster_section=cluster_section)
    ctx.invoke(delete_profile, profile_section=profile_section)

    assert len(proxy.clusters) == 0, \
        'After delete_cluster no profiles should exist'


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
        'Two instances should have been created'

    for instance in [middle[i] for i in instance_ids]:
        assert instance['State'] == 'running', \
            'Instance {} is not running'.format(instance['ID'])
        assert instance['Type'] == 't2.nano', \
            'Instance {} is not a t2.nano instance'.format(instance['ID'])

    ctx.invoke(terminate_cluster, profile_section=profile_section,
               cluster_section=cluster_section)

    ctx.invoke(delete_cluster, cluster_section=cluster_section)
    ctx.invoke(delete_profile, profile_section=profile_section)

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
    assert len(proxy.profiles) == 0, \
        'Profile already exist!'
    assert len(proxy.clusters) == 0, \
        'Clusters already exist!'
    assert len(proxy.volumes) == 0, \
        'Volumes already exist!'

    ctx.invoke(create_profile, profile_section=profile_section)
    ctx.invoke(create_cluster, cluster_section=cluster_section)

    ctx.invoke(launch_cluster, profile_section=profile_section,
               cluster_section=cluster_section)

    begin = get_volume_hash(proxy)

    ctx.invoke(create_volume, profile_section=profile_section,
               volume_section=volume_section)

    ctx.invoke(attach_volume, profile_section=profile_section,
               cluster_section=cluster_section,
               volume_section=volume_section)

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

    ctx.invoke(detach_volume, profile_section=profile_section,
               cluster_section=cluster_section,
               volume_section=volume_section)

    after_detach = get_volume_hash(proxy)

    # Remote state is 'available'
    assert after_detach[vol_id]['State'] == 'available'

    girder_vol = proxy.volumes[0]
    girder_cluster = proxy.clusters[0]

    # local girder status is available
    assert girder_vol['status'] == 'available'

    # volume has been removed from local girder cluster's volume list
    assert girder_vol['_id'] not in girder_cluster['volumes']

    ctx.invoke(delete_volume, profile_section=profile_section,
               volume_section=volume_section)

    after = get_volume_hash(proxy)

    # Removed out on AWS
    assert vol_id not in after.keys()

    # Removed locally
    assert len(proxy.volumes) == 0

    ctx.invoke(terminate_cluster, profile_section=profile_section,
               cluster_section=cluster_section)

    ctx.invoke(delete_cluster, cluster_section=cluster_section)
    ctx.invoke(delete_profile, profile_section=profile_section)


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

    logging.info('Running simple taskflow ...')
    taskflow_id = create_taskflow(
        proxy, 'cumulus.taskflow.core.test.mytaskflows.SimpleTaskFlow')

    # Start the task flow
    proxy.put('taskflows/%s/start' % (taskflow_id))
    wait_for_taskflow_status(proxy, taskflow_id, 'complete')


@cli.command()
@test_case
def test_linked_taskflow(ctx, proxy):

    logging.info('Running linked taskflow ...')
    taskflow_id = create_taskflow(
        proxy, 'cumulus.taskflow.core.test.mytaskflows.LinkTaskFlow')

    # Start the task flow
    proxy.put('taskflows/%s/start' % (taskflow_id))
    wait_for_taskflow_status(proxy, taskflow_id, 'complete')


@cli.command()
@test_case
def test_terminate_taskflow(ctx, proxy):
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
    from StringIO import StringIO

    logging.info('Starting traditional cluster test...')
    proxy.cluster_section = cluster_section

    if host is not None:
        proxy.cluster_host = host

    if port is not None:
        proxy.cluster_port = port

    ctx.invoke(create_profile, profile_section=profile_section)
    ctx.invoke(create_cluster, cluster_section=None)

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


    import pudb; pu.db

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
    ctx.invoke(delete_cluster, cluster_section=cluster_section)
    ctx.invoke(delete_profile, profile_section=profile_section)




if __name__ == '__main__':
    try:
        cli()
    except SystemExit:
        pass

    report()
