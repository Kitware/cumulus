import re
from cumulus.queue.abstract import AbstractQueueAdapter
from cumulus.constants import JobQueueState


class SgeQueueAdapter(AbstractQueueAdapter):
    # Running states
    RUNNING_STATE = ['r', 'd']

    ERROR_STATE = ['e']

    # Queued states
    QUEUED_STATE = ['qw', 'q', 'w', 's', 'h', 't']

    def terminate_job(self, job):
        command = 'qdel %s' % job['queueJobId']
        output = self._cluster_connection.execute(command)

        return output

    def _parse_job_id(self, submit_output):
        m = re.match('^[Yy]our job (\\d+)', submit_output[0])
        if not m:
            raise Exception('Unable to extraction job id from: %s'
                            % submit_output[0])
        sge_id = m.group(1)

        return sge_id

    def submit_job(self, job, job_script):
        command = 'cd %s && qsub -cwd ./%s' % (job['dir'], job_script)
        output = self._cluster_connection.execute(command)

        if len(output) != 1:
            raise Exception('Unexpected qsub output: %s' % output)

        return self._parse_job_id(output)

    def job_status(self, job):
        output = self._cluster_connection.execute('qstat')

        state = None
        sge_state = self._extract_job_status(output, job)

        if sge_state:
            if sge_state in SgeQueueAdapter.RUNNING_STATE:
                state = JobQueueState.RUNNING
            elif sge_state in SgeQueueAdapter.ERROR_STATE:
                state = JobQueueState.ERROR
            elif sge_state in SgeQueueAdapter.QUEUED_STATE:
                state = JobQueueState.QUEUED

        return state

    def _extract_job_status(self, job_status_output, job):
        state = None
        job_id = job[AbstractQueueAdapter.QUEUE_JOB_ID]
        for line in job_status_output:
            m = re.match('^\\s*(\\d+)\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\w+)',
                         line)
            if m and m.group(1) == job_id:
                state = m.group(2)

        return state

    def is_running(self, state):
        return state in SgeQueueAdapter.RUNNING_STATE

    def is_queued(self, state):
        return state in SgeQueueAdapter.QUEUED_STATE
