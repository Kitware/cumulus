
import mock
from threading import Timer
from functools import partial

from girder.api import access

import cumulus.starcluster.tasks as tasks
import girder.plugins.cumulus

class Cluster(girder.plugins.cumulus.Cluster):

    def _update_status(self, id, status):
        cluster = self._model.load(id, force=True)
        cluster['status'] = status
        self._model.save(cluster)

    def _update_job_status(self, id, status):
        model = self.model('job', 'cumulus')
        job = model.load(id, force=True)
        job['status'] = status
        model.save(job)

    @access.user
    def start(self, id, params):

        with mock.patch('cumulus.starcluster.tasks.cluster.start_cluster.delay') as delay:
            super(Cluster, self).start(id, params)
            update_status = partial(self._update_status, id, 'running')
            t = Timer(10, update_status)
            t.start()

    @access.user
    def terminate(self, id, params):

        with mock.patch('cumulus.starcluster.tasks.cluster.terminate_cluster.delay') as delay:
            super(Cluster, self).terminate(id, params)
            self._update_status(id, 'terminating')
            update_status = partial(self._update_status, id, 'terminated')
            t = Timer(10, update_status)
            t.start()


    @access.user
    def submit_job(self, id, jobId, params):

        with mock.patch('cumulus.starcluster.tasks.job.submit') as submit:
            super(Cluster, self).submit_job(id, jobId, params)
            self._update_job_status(jobId, 'queued')
            def _running():
                self._update_job_status(jobId, 'running')
                update_status = partial(self._update_job_status, jobId, 'complete')
                t = Timer(10, update_status)
                t.start()
            t = Timer(10, _running)
            t.start()



