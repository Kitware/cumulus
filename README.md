# cumulus
A RESTful API for creating and using clusters in the cloud.

The goal of the project is to provide a platform for developing HPC workflows. Cumulus enables running workflows on traditional or on-demand clusters. It provides the ability to create compute resources on cloud computing platforms such as AWS, and then to provision MPI clusters on top of those resources using Ansible.

Job management is supported through traditional batch job schedulers. We also provide a workflow engine called TaskFlow to allow a collection of Celery tasks to be run as a workflow.

## Girder plugins

Cumulus is made up of four [Girder](https://github.com/girder/girder) plugins, and so must be run under an existing Girder server.

### cumulus

The `cumulus` plugin provides the RESTful API to create and interact with clusters, and to manage jobs.

### taskflow

The `taskflow` plugin defines workflow orchestration tooling for Celery Tasks running from Girder.

Submitting a job to an HPC cluster isn't a simple matter, e.g. you must create and provision the cluster, upload data to the cluster, get your data in the right format, monitor the job when it is running, check the status when it is finished, download the output, convert the data to the correct format for visualization, and then visualize the output.  

TaskFlow is the attempt to take a set of celery tasks and reflect their state in Girder, as such it is a thin neveer on top of Celery's existing capabilities, e.g. Chords and Workflows.

### SFTP assetstore

The `sftp` plugin implements a Girder assetstore type based on an SFTP server instance, most likely running on a cluster, and streams data off of the cluster on demand.

### NEWT assetstore

The `newt` plugin implements a Girder assetstore type based on NEWT, a RESTful API used by [NERSC](http://www.nersc.gov/) as a filesystem abstraction and interface to job submission and monitoring for several HPC clusters housed at NERSC.

### SFTP and NEWT as proxy assetstores

Both of these assetstore types are proxy or unmanaged assetstores, in that they allow navigation of the file hierarchy within Girder, but do not actually download or import the bytes of the files into a Girder managed assetstore. Girder will import the names, hierarchy and metadata from a file tree, but will not import the files themselves. When a particular file is downloaded through the Girder UI, the file will be downloaded from the upstream filesystem abstraction (SFTP or NEWT).

TODO: will the bytes ever move from the upstream assetstore to a Girder managed assetstore?  or only from the upstream to a requesting client?

## Credential management in Cumulus

For an EC2 cluster, a user has an existing AWS IAM user they want to use.  They will register their AWS credentials for this AWS user with Girder, the credentials are then saved to their Girder user, a Profile (a Girder model created in `cumulus`) creates an AWS key-pair using the AWS credentials, then downloads that key-pair, saving it in the Profile.  A single user might have multiple profiles, e.g. to target different clusters or different AWS accounts.

TODO: some mention of security is in order here.  How are key-pairs and credentials kept safely?

For a traditional cluster, the user registers a cluster and Cumulus creates a key-pair for that user-cluster pairing. It is up to the user (the person) to take the public-key part of key pair and put it in the appropriate authorized_keys file connected to their user on the cluster itself.  This gives Cumulus the ability to run commands on the cluster as that user.  The Cluster (a Girder model created in `cumulus`) is where the key pair is saved.

TODO: how does a user register a cluster, some connection info presumably?
