import re
from cumulus.queue.abstract import AbstractQueueAdapter
from cumulus.constants import JobQueueState


class SlurmQueueAdapter(AbstractQueueAdapter):

    # CA  CANCELLED       Job  was explicitly cancelled by the user or system
    #                     administrator.  The job may or may  not  have  been
    #                     initiated.
    # CD  COMPLETED       Job has terminated all processes on all nodes.
    # CF  CONFIGURING     Job  has  been allocated resources, but are waiting
    #                     for them to become ready for use (e.g. booting).
    # CG  COMPLETING      Job is in the process of completing. Some processes
    #                     on some nodes may still be active.
    # F   FAILED          Job  terminated  with  non-zero  exit code or other
    #                     failure condition.
    # NF  NODE_FAIL       Job terminated due to failure of one or more  allo-
    #                     cated nodes.
    # PD  PENDING         Job is awaiting resource allocation.
    # PR PREEMPTED        Job terminated due to preemption.
    # R   RUNNING         Job currently has an allocation.
    # S   SUSPENDED       Job  has an allocation, but execution has been sus-
    #                     pended.
    # TO  TIMEOUT         Job terminated upon reaching its time limit.

    # Running states
    RUNNING_STATE = ['ca', 'cg', 'r', 's', 'to']

    ERROR_STATE = ['f', 'nf']

    COMPLETE_STATE = ['cd', 'pr']

    # Queued states
    QUEUED_STATE = ['cf', 'pd']

    def terminate_job(self, job):
        command = 'scancel %s' % job['queueJobId']
        output = self._cluster_connection.execute(command)

        return output

    def _parse_job_id(self, submit_output):
        m = re.match('^Submitted batch job (\\d+)', submit_output[0])
        if not m:
            raise Exception('Unable to extraction job id from: %s'
                            % submit_output[0])
        slurm_id = m.group(1)

        return slurm_id

    def submit_job(self, job, job_script):
        command = 'cd %s && sbatch ./%s' % (job['dir'], job_script)
        output = self._cluster_connection.execute(command)

        if len(output) != 1:
            raise Exception('Unexpected sbatch output: %s' % output)

        return self._parse_job_id(output)

    def job_statuses(self, jobs):
        job_ids = ','.join(
            [job[AbstractQueueAdapter.QUEUE_JOB_ID] for job in jobs])
        output = self._cluster_connection.execute('squeue -j %s'
                                                  % job_ids)

        return self._extract_job_statuses(output, jobs)

    def to_job_queue_state(self, slurm_state):
        state = None
        slurm_state = slurm_state.lower() if slurm_state else slurm_state
        if slurm_state in SlurmQueueAdapter.RUNNING_STATE:
            state = JobQueueState.RUNNING
        elif slurm_state in SlurmQueueAdapter.ERROR_STATE:
            state = JobQueueState.ERROR
        elif slurm_state in SlurmQueueAdapter.QUEUED_STATE:
            state = JobQueueState.QUEUED
        elif slurm_state in SlurmQueueAdapter.COMPLETE_STATE:
            state = JobQueueState.COMPLETE

        return state

    def _extract_job_statuses(self, job_status_output, jobs):
        states = []
        for job in jobs:
            slurm_state = self._extract_job_status(job_status_output, job)
            state = self.to_job_queue_state(slurm_state)
            states.append((job, state))

        return states

    def _extract_job_status(self, job_status_output, job):
        state = None
        job_id = job[AbstractQueueAdapter.QUEUE_JOB_ID]
        for line in job_status_output:
            m = re.match('^\\s*(\\d+)\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\w+)',
                         line)

            if m and m.group(1) == job_id:
                state = m.group(2).lower()
                break

        return state
