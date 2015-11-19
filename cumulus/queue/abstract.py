class AbstractQueueAdapter(object):
    QUEUE_JOB_ID = 'queueJobId'

    def __init__(self, cluster, cluster_connection):
        self._cluster = cluster
        self._cluster_connection = cluster_connection

    def submit_job(self, job, job_script):
        raise NotImplementedError('Subclasses should implement this')

    def terminate_job(self, job):
        raise NotImplementedError('Subclasses should implement this')

    def job_status(self, job):
        raise NotImplementedError('Subclasses should implement this')

    def is_running(self, state):
        raise NotImplementedError('Subclasses should implement this')

    def is_queued(self, state):
        raise NotImplementedError('Subclasses should implement this')
