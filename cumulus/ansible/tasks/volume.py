from cumulus.celery import command
from cumulus.ansible.tasks.dynamic_inventory.ec2 import get_inventory
from utils import logger, run_playbook, get_playbook_directory
import os

@command.task
def create_volume(cluster, profile, secret_key, girder_token, log_write_url):
    playbook = os.path.join([get_playbook_directory(), "volumes",
                             "ec2", "create"])
