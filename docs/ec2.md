## Credential management in Cumulus

For an EC2 cluster, a user has an existing AWS IAM user they want to use.  They will register their AWS credentials for this AWS user with Girder, the credentials are then saved to their Girder user, a Profile (a Girder model created in `cumulus`) creates an AWS key-pair using the AWS credentials, then downloads that key-pair, saving it in the Profile.  A single user might have multiple profiles, e.g. to target different clusters or different AWS accounts.

TODO: some mention of security is in order here.  How are key-pairs and credentials kept safely?

For a traditional cluster, the user registers a cluster and Cumulus creates a key-pair for that user-cluster pairing. It is up to the user (the person) to take the public-key part of key pair and put it in the appropriate authorized_keys file connected to their user on the cluster itself.  This gives Cumulus the ability to run commands on the cluster as that user.  The Cluster (a Girder model created in `cumulus`) is where the key pair is saved.

TODO: how does a user register a cluster, some connection info presumably?
