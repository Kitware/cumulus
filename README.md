# cumulus
A platform for building HPC workflows.

The goal of the project is to provide a platform for developing HPC workflows. Cumulus enables running workflows on traditional or on-demand clusters. It provides the ability to create compute resources on cloud computing platforms such as AWS, and then to provision MPI clusters on top of those resources using Ansible.

Job management is supported through traditional batch job schedulers. We also provide a workflow engine called TaskFlow to allow a collection of Celery tasks to be run as a workflow.

See [documentation](docs/README.md) for more details.



