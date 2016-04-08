# How to get started with an ansible cluster

## Get your user ID
   First you will need to get your user id from the system. Login and go to the api (usually at http://127.0.0.1:8080/api/v1/).  Under ```user``` there is an endpoint user/me.  Click "Try it out!" and copy the user id.

## Setup profile
   Next set up a profile:  Under ```user``` there is a POST endpoint ```/user/{id}/aws/profiles``` This takes a json object withe the following keys:
```json
    {
      "accessKeyId": "<<ACCESSKEYID>>",
      "availabilityZone": "us-west-2a",
      "name": "My Profile",
      "regionName": "us-west-2",
      "secretAccessKey": "<<SECRETACCESSKEYID>>"
    }
```

```name``` must be unique. This will return a profile,  copy the id from the ```_id``` field. you will have to supply your AWS access key and secret access key.

## Create a cluster
   Finally create the cluster under the ```cluster``` endpoint with the following json:

```json
    {"name": "test cluster",
     "type": "ansible",
     "config": {
       "launch": {
         "spec": "ec2",
         "params": {
           "master_instance_type": "t2.micro",
           "master_instance_ami": "ami-03de3c63",
           "node_instance_count": 2,
           "node_instance_type": "t2.micro",
           "node_instance_ami": "ami-03de3c63"
         }
       }
     },
     "profileId": "<<profile_id>>"}
```

+ ```master_instance_type``` describes the instance type of the master node,
+ ```master_instance_ami``` describes the instance ami to use for the master node
+ ```node_instance_type``` describes the instance type of the slave nodes,
+ ```node_instance_ami``` describes the instance ami to use for the slave nodes
+ ```node_instance_count``` the number of slave nodes to start.


Other variables may be passed into config.launch.params.  These will override any variables set in the playbook or used by ansible.

## Launch the cluster
Hit the **/cluster/<<cluster_id>>/launch** endpoint

## Provision the cluster
Hit the **/cluster/<<cluster_id>>/provision** endpoint. You must pass in data that includes a spec (aka playbook), e.g.:

```json
  { "spec": "gridengine/site",
    "ssh": {
      "user": "ubuntu"
    }
  }
```
+ ```ssh.user``` login that ansible will use for ssh
