import re
from cumulus.queue.abstract import AbstractQueueAdapter


class SgeQueueAdapter(AbstractQueueAdapter):
    # Running states
    RUNNING_STATE = ['r', 'd', 'e']

    # Queued states
    QUEUED_STATE = ['qw', 'q', 'w', 's', 'h', 't']

    @classmethod
    def terminate_job_command(cls, job):
        return 'qdel %s' % job['queueJobId']

    @classmethod
    def parse_job_id(cls, submit_output):
        m = re.match('^[Yy]our job (\\d+)', submit_output[0])
        if not m:
            raise Exception('Unable to extraction job id from: %s'
                            % submit_output[0])
        sge_id = m.group(1)

        return sge_id

    @classmethod
    def submit_job_command(cls, job_script):
        return 'qsub -cwd ./%s' % job_script

    @classmethod
    def job_status_command(cls, job):
        return 'qstat'

    @classmethod
    def extract_job_status(cls, job_status_output, job):
        state = None
        job_id = job[cls.QUEUE_JOB_ID]
        for line in job_status_output:
            m = re.match('^\\s*(\\d+)\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\w+)',
                         line)
            if m and m.group(1) == job_id:
                state = m.group(2)

        return state

    @classmethod
    def is_running(cls, state):
        return state in cls.RUNNING_STATE

    @classmethod
    def is_queued(cls, state):
        return state in cls.QUEUED_STATE
