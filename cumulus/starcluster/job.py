from cumulus.starcluster.tasks import  download_job_input, submit_job

def submit(cluster, job, base_url, log_url, config_url, girder_token):

    # Do we inputs to download ?
    if 'input' in job and len(job['input']) > 0:

        download_job_input.delay(cluster, job, base_url=base_url,
                           log_write_url=log_url, config_url=config_url,
                           girder_token=girder_token)
    else:
        submit_job.delay(cluster, job,
                         log_write_url=log_url,  config_url=config_url,
                         girder_token=girder_token,
                         base_url=base_url)
