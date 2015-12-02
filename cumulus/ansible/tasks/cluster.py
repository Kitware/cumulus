from cumulus.celery import command


@command.task
def deploy_cluster(cluster, girder_token, log_write_url):
    from pudb.remote import set_trace; set_trace(term_size=(46, 185))
