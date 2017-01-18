# Cumulus command line script

Cumulus has a command line script that works by communicating with girder's REST endpoints to execute various components of architecture. This command line script can be used to:

+ Create, list, and delete cumulus **profiles**
+ Create, list, launch, terminate, and delete cumulus **clusters**
+ Create, list, attach, detach, and delete cumulus **volumes**
+ List AWS instances
+ List AWS volumes

This is intended primarily for debugging and [integration tests](../../tests/integration/README.md), but may be useful outside of these contexts. Because of this it has been seperated from the integration tests and exists as its own standalone tool.

## Usage

Once the cumulus python package has been installed the tool should be available as 'cumulus.'  Running ```cumulus --help``` should provide a list of basic operations. A great deal of user input is required to create profiles, clusters and volumes.  The cumulus command expects this information to be available in a configuration file.


## Configuration

By default the cumulus command expects an ```integration.cfg``` to be available in the current working directory.  Different path locations can be specified by using the --config option to cumulus (e.g.  ```cumulus --config=/path/to/integration.cfg [COMMAND]```). An example of the configuration file is [available](../../tests/integration/integration.example.cfg). By default cumulus will read sections out of this configuration file and use them to run REST requests against girder.  

When creating a profile for instance, the cumulus command will look in the ```[profile]``` section of the configuration for information like the ```name``` and ```cloudProvider``` of the profile. What section the cumulus command looks in for profile information can easily be changed via the ```--profile_section``` option. E.g.:

```sh
cumulus profile --profile_section="other_profile" create
```

This will look in the ```[other_profile]``` section of the configuration for profile variables.  In this way multiple configurations can be stored and shared in the same configuration file. 


## Sub-commands
The cumulus command is divided into a number of sub commands:

### Profile
```
$> cumulus profile [OPTIONS] [COMMAND]
```
Current commands are ```create```, ```list```, and ```delete```

Options are: 

```profile_section```which pulls profile vars from the section name that is passed into the option (default: "profile").

### Cluster
```
$> cumulus cluster [OPTIONS] [COMMAND]
```

Current commands are ```create```, ```list```, ```launch```, ```terminate``` and ```delete```

Options are: 

```profile_section```which pulls profile vars from the section name that is passed into the option (default: "profile").
```cluster_section```which pulls cluster vars from the section name that is passed into the option (default: "cluster").
### Volume
```
$> cumulus volume [OPTIONS] [COMMAND]
```

Current commands are ```create```, ```list```, ```attach```, ```detach``` and ```delete```

Options are:

```profile_section```which pulls profile vars from the section name that is passed into the option (default: "profile").
```cluster_section```which pulls cluster vars from the section name that is passed into the option (default: "cluster").
```volume_section```which pulls volume vars from the section name that is passed into the option (default: "volume").


### AWS
```
$> cumulus aws [COMMAND]
```

Current commands are ```instances``` and ```volumes```

```instances``` will use credential information from the ```[aws]``` section of the config (by default) and make boto requests out to AWS to pull down and print a list of running instances.  This is useful for checking whether or not launch/terminate commands have correctly completed.

```volumes```, like ```instances``` uses the ```[aws]``` section for credential information and prints the current EBS volumes on AWS.

### full_status
```
$> cumulus full_status
```

Combined the outputs of:
```
cumulus profile list
cumulus cluster list
cumulus volume list
cumulus aws instances
cumulus aws volumes
```

## Verbosity
By default the cumulus command will only print errors/warnings. Much useful information is made available however by using the ```-v/--verbose``` option.  E.g.:

```sh
$> cumulus -v cluster create

2017-01-18 18:04:44,028 INFO  - Creating cluster "AnsibleIntegrationTest"
2017-01-18 18:04:44,053 INFO  - Creating profile "testProfile"
2017-01-18 18:04:47,885 INFO  - Finished creating profile "testProfile" (587fae3d0640fd6ca7c7d482)
2017-01-18 18:04:47,951 INFO  - Finished creating cluster "AnsibleIntegrationTest" (587fae3f0640fd6ca7c7d484)
```

Even basic ansible output is forwarded to the tool. E.g.:

```sh
$> cumulus -v cluster launch

2017-01-18 18:04:56,649 INFO  - Launching cluster "AnsibleIntegrationTest"
2017-01-18 18:04:57,797 INFO  - ANSIBLE - TASK: ec2-pod : include (starting)
2017-01-18 18:04:57,798 INFO  - ANSIBLE - TASK: ec2-pod : include (skipped)
2017-01-18 18:04:57,798 INFO  - ANSIBLE - TASK: ec2-pod : include (starting)
2017-01-18 18:04:57,798 INFO  - ANSIBLE - TASK: ec2-pod : Fail early if required variables are not defined (starting)
2017-01-18 18:04:57,798 INFO  - ANSIBLE - TASK: ec2-pod : Fail early if required variables are not defined (skipped)
2017-01-18 18:04:57,798 INFO  - ANSIBLE - TASK: ec2-pod : Define default firewall rules if not provided (starting)
2017-01-18 18:04:58,821 INFO  - ANSIBLE - TASK: ec2-pod : Define default firewall rules if not provided (finished)
2017-01-18 18:04:58,821 INFO  - ANSIBLE - TASK: ec2-pod : Find latest Master AMI (starting)
2017-01-18 18:04:58,822 INFO  - ANSIBLE - TASK: ec2-pod : Find latest Master AMI (skipped)
2017-01-18 18:04:58,822 INFO  - ANSIBLE - TASK: ec2-pod : Register to master_instance_ami variable (starting)
2017-01-18 18:04:58,822 INFO  - ANSIBLE - TASK: ec2-pod : Register to master_instance_ami variable (skipped)
2017-01-18 18:04:58,822 INFO  - ANSIBLE - TASK: ec2-pod : Find latest node AMI (starting)
2017-01-18 18:04:58,822 INFO  - ANSIBLE - TASK: ec2-pod : Find latest node AMI (skipped)
2017-01-18 18:04:58,822 INFO  - ANSIBLE - TASK: ec2-pod : Register to node_instance_ami variable (starting)
2017-01-18 18:04:58,822 INFO  - ANSIBLE - TASK: ec2-pod : Register to node_instance_ami variable (skipped)
2017-01-18 18:04:58,822 INFO  - ANSIBLE - TASK: ec2-pod : Create a custom security group (starting)
2017-01-18 18:05:00,874 INFO  - ANSIBLE - TASK: ec2-pod : Create a custom security group (finished)
2017-01-18 18:05:00,874 INFO  - ANSIBLE - TASK: ec2-pod : Register custom security group (starting)
2017-01-18 18:05:01,895 INFO  - ANSIBLE - TASK: ec2-pod : Register custom security group (finished)
2017-01-18 18:05:01,895 INFO  - ANSIBLE - TASK: ec2-pod : Launch master instance (starting)
2017-01-18 18:05:04,965 INFO  - ANSIBLE - TASK: ec2-pod : Launch master instance (finished)
2017-01-18 18:05:04,965 INFO  - ANSIBLE - TASK: ec2-pod : Launch node instances (starting)
2017-01-18 18:05:09,084 INFO  - ANSIBLE - TASK: ec2-pod : Launch node instances (finished)
2017-01-18 18:05:09,085 INFO  - ANSIBLE - TASK: ec2-pod : Poll instance data to get public DNS names (starting)
2017-01-18 18:05:10,119 INFO  - ANSIBLE - TASK: ec2-pod : Poll instance data to get public DNS names (finished)
2017-01-18 18:05:10,120 INFO  - ANSIBLE - TASK: ec2-pod : Wait for SSH to come up on all instances (starting)
2017-01-18 18:05:59,384 INFO  - ANSIBLE - TASK: ec2-pod : Wait for SSH to come up on all instances (finished)
2017-01-18 18:06:00,419 INFO  - Finished launching cluster AnsibleIntegrationTest  (587fae3f0640fd6ca7c7d484)
```

