import six


def flatten_ec2_result(ec2_result):
    result = []
    for entry in ec2_result['results']:
        for instance in entry['tagged_instances']:
            result.append({'hostname': instance['public_dns_name'],
                           'id': instance['id'],
                           'groups': entry['item']['value']['groups']})

    return result


def process_hosts_spec(hosts_spec, pod_name):
    result = {}
    for key, value in hosts_spec.items():
        value['groups'] = list(set(value.get('groups', ())) |
                               set((pod_name,)))

        if 'volumes' in value:
            new_volumes = []
            for volume_name, volume_size in value['volumes'].items():
                new_volumes.append({'delete_on_termination': True,
                                    'device_name': '/dev/' + volume_name,
                                    'volume_size': volume_size})

            value['volumes'] = new_volumes

        result[key] = value

    return result


def compute_ec2_update_lists(pod_name,
                             hosts_spec,
                             aws_access_key,
                             aws_secret_key,
                             state,
                             region,
                             default_ssh_key,
                             default_image,
                             default_instance_type):

    from collections import defaultdict
    from itertools import chain
    from boto import ec2

    conn = ec2.connect_to_region(region, aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_key)
    if conn is None:
        raise Exception(' '.join((
            'region name:',
            region,
            'likely not supported, or AWS is down.'
            'connection to region failed.')))

    reservations = conn.get_all_instances()

    # short-circuit the case where the pod should be terminated
    if state == 'absent':
        return {'start': [], 'terminate': list(set(
            chain.from_iterable(
                (instance.id for instance in reservation.instances
                 if instance.tags.get('ec2_pod') == pod_name)
                for reservation in reservations)
        ))}

    ec2_host_table = defaultdict(lambda: defaultdict(set))
    for reservation in reservations:
        for instance in reservation.instances:
            if instance.tags.get('ec2_pod') != pod_name:
                continue

            if instance.state not in ('running', 'stopped'):
                continue

            instance_name = instance.tags.get('ec2_pod_instance_name')
            composite_key = (six.text_type(instance_name),
                             six.text_type(instance.key_name),
                             six.text_type(instance.image_id),
                             six.text_type(instance.instance_type))

            ec2_host_table[composite_key][instance.state].add(instance.id)

    host_counter_table = dict(
        ((six.text_type(key),
          six.text_type(value.get('ssh_key', default_ssh_key)),
          six.text_type(value.get('image', default_image)),
          six.text_type(value.get('type', default_instance_type))),
         value.get('count', 1))
        for key, value in hosts_spec.items())

    start_set = set()
    terminate_set = set()

    for composite_key, sets in ec2_host_table.items():
        running_list = list(sets['running'])
        stopped_list = list(sets['stopped'])

        num_running = len(running_list)
        # num_stopped = len(stopped_list)

        num_wanted = host_counter_table.get(composite_key, 0)

        num_to_keep = min(num_running, num_wanted)
        num_to_start = num_wanted - num_to_keep

        start_set |= set(stopped_list[:num_to_start])

        terminate_set |= set(stopped_list[num_to_start:])
        terminate_set |= set(running_list[num_to_keep:])

    return {'start': list(start_set), 'terminate': list(terminate_set)}


def get_ec2_hosts(instance_table):
    import operator as op
    return map(op.itemgetter('id'), instance_table)


class FilterModule(object):
    def filters(self):
        return {'compute_ec2_update_lists': compute_ec2_update_lists,
                'flatten_ec2_result': flatten_ec2_result,
                'get_ec2_hosts': get_ec2_hosts,
                'process_hosts_spec': process_hosts_spec}
