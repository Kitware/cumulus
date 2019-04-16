import re
from cumulus.queue.abstract import AbstractQueueAdapter
from cumulus.constants import JobQueueState


class PbsQueueAdapter(AbstractQueueAdapter):
    # Running states
    RUNNING_STATE = ['r']

    ERROR_STATE = ['e']

    COMPLETE_STATE = ['c']

    # Queued states
    QUEUED_STATE = ['q', 'h', 't', 'w', 's']

    def terminate_job(self, job):
        command = 'qdel %s' % job['queueJobId']
        output = self._cluster_connection.execute(command)

        return output

    def _parse_job_id(self, submit_output):
        m = re.match(r'^(\d+)\..*', submit_output[0])
        if not m:
            raise Exception('Unable to extraction job id from: %s'
                            % submit_output[0])
        sge_id = m.group(1)

        return sge_id

    def submit_job(self, job, job_script):
        command = 'cd %s && qsub ./%s' % (job['dir'], job_script)
        output = self._cluster_connection.execute(command)

        if len(output) != 1:
            raise Exception('Unexpected qsub output: %s' % output)

        return self._parse_job_id(output)

    def job_statuses(self, jobs):

        job_ids = ' '.join(
            [job[AbstractQueueAdapter.QUEUE_JOB_ID] for job in jobs])

        output = self._cluster_connection.execute('qstat %s' % job_ids)

        states = []
        for job in jobs:
            state = None
            pbs_state = self._extract_job_status(output, job)

            if pbs_state:
                if pbs_state in PbsQueueAdapter.RUNNING_STATE:
                    state = JobQueueState.RUNNING
                elif pbs_state in PbsQueueAdapter.ERROR_STATE:
                    state = JobQueueState.ERROR
                elif pbs_state in PbsQueueAdapter.QUEUED_STATE:
                    state = JobQueueState.QUEUED
                elif pbs_state in PbsQueueAdapter.COMPLETE_STATE:
                    state = JobQueueState.COMPLETE

            states.append((job, state))

        return states

    def _extract_job_status(self, job_status_output, job):
        state = None
        job_id = job[AbstractQueueAdapter.QUEUE_JOB_ID]
        for line in job_status_output:
            m = re.match(r'^\s*(\d+)\S*\s+\S+\s+\S+\s+\S+\s+(\w+)',
                         line)

            if m and m.group(1) == job_id:
                state = m.group(2).lower()
                break

        return state
