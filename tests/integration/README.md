# Cumulus Integration Tests

Cumulus has integration test infrastructure that is based on the cumulus [command line script](../../cumulus/scripts/README.md) it can be fun from this directory by copying ```integration.example.cfg``` to ```integration.cfg``` and adding AWS credentials under the ```[aws]``` section of the file. Once copied,  tests may be run by executing:

```sh
$> python integration.py -v [TEST]
```

Where ```[TEST]``` is one of the following:

## test_profile
Test profile creation/deletion.

This will:

+ create a profile
+ delete the profile

## test_cluster
Test cluster creation/deletion.

This will:

+ create a profile
+ create a cluster
+ delete the cluster
+ delete the profile

## test_launch_cluster
Test launching/terminating a cluster.

This will: 

+ create a profile
+ create a cluster
+ launch the cluster
+ query AWS and make assertions about instance state
+ terminate the cluster
+ query AWS and make assertions about instance state
+ delete the cluster
+ delete the profile

## test_volume 
Test attaching/detaching/deleting a volume

This will:

+ create a profile
+ create a cluster
+ launch the cluster
+ create a volume
+ attach the volume
+ query AWS and make assertions about the volume
+ detach the volume
+ query AWS and make assertions about the volume
+ delete the volume
+ terminate the cluster
+ delete the cluster
+ delete the profile


## test_taskflow
Run all taskflow tests

### test_simple_taskflow
Test a simple taskflow

### test_linked_taskflow
Test a linked taskflow

### test_chord_taskflow
Test running a taskflow with a chord

### test_connected_taskflow
Test a connected taskflow

### test_terminate_taskflow
Test terminating a taskflow

## test_traditional
Test creating a traditional cluster


