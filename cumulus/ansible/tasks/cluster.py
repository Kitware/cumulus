from cumulus.celery import command
import ansible.playbook
from ansible import errors
from ansible import callbacks
from ansible import utils
import os

def display(msg, color=None, stderr=False, screen_only=False, log_only=False, runner=None):
    pass



@command.task
def deploy_cluster(cluster, girder_token, log_write_url):
    # from pudb.remote import set_trace; set_trace(term_size=(185, 46))

    playbook_path = os.path.dirname(__file__) + "/../playbooks/default.yml"

    # Get the inventory
    inventory = ansible.inventory.Inventory(['localhost'])


    stats = callbacks.AggregateStats()
    # verbose=1  equivalent to passing one '-v' on command line
    playbook_cb = callbacks.PlaybookCallbacks(verbose=1)
    runner_cb = callbacks.PlaybookRunnerCallbacks(stats, verbose=1)

    extra_vars = {
        "girder_token": girder_token,
        "log_write_url": log_write_url
    }

    pb = ansible.playbook.PlayBook(
        playbook=playbook_path,
        inventory=inventory,
        callbacks=playbook_cb,
        runner_callbacks=runner_cb,
        stats=stats,
        extra_vars=extra_vars
    )

    # Note:  can refer to callback.playbook.extra_vars  to get access
    # to girder_token after this point

    pb.run()
