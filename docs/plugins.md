## Girder plugins

Cumulus is made up of four [Girder](https://github.com/girder/girder) plugins, and so must be run under an existing Girder server.

### cumulus

The `cumulus` plugin provides the RESTful API to create and interact with clusters, and to manage jobs.

### taskflow

The `taskflow` plugin defines workflow orchestration tooling for Celery Tasks running from Girder.

Submitting a job to an HPC cluster isn't a simple matter, e.g. you must create and provision the cluster, upload data to the cluster, get your data in the right format, monitor the job when it is running, check the status when it is finished, download the output, convert the data to the correct format for visualization, and then visualize the output.

TaskFlow is the attempt to take a set of celery tasks and reflect their state in Girder, as such it is a thin wrapper on top of Celery's existing capabilities, e.g. Chords and Workflows.

### SFTP assetstore

The `sftp` plugin implements a Girder assetstore type based on an SFTP server instance, most likely running on a cluster, and streams data off of the cluster on demand.

### NEWT assetstore

The `newt` plugin implements a Girder assetstore type based on NEWT, a RESTful API used by [NERSC](http://www.nersc.gov/) as a filesystem abstraction and interface to job submission and monitoring for several HPC clusters housed at NERSC.

### SFTP and NEWT as proxy assetstores

Both of these assetstore types are proxy or unmanaged assetstores, in that they allow navigation of the file hierarchy within Girder, but do not actually download or import the bytes of the files into a Girder managed assetstore. Girder will import the names, hierarchy and metadata from a file tree, but will not import the files themselves. When a particular file is downloaded through the Girder UI, the file will be downloaded from the upstream filesystem abstraction (SFTP or NEWT).

TODO: will the bytes ever move from the upstream assetstore to a Girder managed assetstore?  or only from the upstream to a requesting client?
