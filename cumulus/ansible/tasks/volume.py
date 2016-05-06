from cumulus.celery import command
from cumulus.ansible.tasks.inventory import AnsibleInventory

from cumulus.ansible.tasks.utils import run_playbook
from cumulus.ansible.tasks.utils import get_playbook_directory
from cumulus.ansible.tasks.utils import get_callback_plugins_path
from cumulus.ansible.tasks.utils import get_library_path

import os
from cumulus.ssh.tasks.key import _key_path


@command.task
def create_volume(profile, volume, secret_key, girder_callback_info):

    playbook = os.path.join(get_playbook_directory(),
                            "volumes", "ec2", "create.yml")

    extra_vars = {
        "girder_volume_id": volume["_id"],
        "profile_id": profile['_id'],
        "volume_name": volume['name'],
        "volume_size": volume['size'],
        "volume_zone": volume['zone']
    }
    extra_vars.update(girder_callback_info)

    env = os.environ.copy()
    env.update({'AWS_ACCESS_KEY_ID': profile['accessKeyId'],
                'AWS_SECRET_ACCESS_KEY': secret_key,
                'ANSIBLE_HOST_KEY_CHECKING': 'false',
                'ANSIBLE_CALLBACK_PLUGINS': get_callback_plugins_path(),
                'ANSIBLE_LIBRARY':get_library_path(),
                'PRIVATE_KEY_FILE': _key_path(profile)})

    inventory = AnsibleInventory(['localhost'])

    with inventory.to_tempfile() as inventory_path:
        run_playbook(playbook, inventory_path,
                     extra_vars, verbose=3, env=env)
