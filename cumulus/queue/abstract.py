class AbstractQueueAdapter(object):
    QUEUE_JOB_ID = 'queueJobId'

    @classmethod
    def submit_job_command(cls, job_script):
        raise NotImplementedError('Subclasses should implement this')

    @classmethod
    def terminate_job_command(cls, job):
        raise NotImplementedError('Subclasses should implement this')

    @classmethod
    def job_status_command(cls, job):
        raise NotImplementedError('Subclasses should implement this')

    @classmethod
    def parse_job_id(cls, submit_output):
        raise NotImplementedError('Subclasses should implement this')

    @classmethod
    def extract_job_status(cls, job_status_output, job):
        raise NotImplementedError('Subclasses should implement this')

    @classmethod
    def is_running_state(cls, state):
        raise NotImplementedError('Subclasses should implement this')

    @classmethod
    def is_queued_state(cls, state):
        raise NotImplementedError('Subclasses should implement this')
