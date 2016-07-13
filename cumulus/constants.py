#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2015 Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the "License" );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################


from enum import IntEnum
import six


class ClusterType:
    EC2 = 'ec2'
    ANSIBLE = 'ansible'
    TRADITIONAL = 'trad'
    NEWT = 'newt'

    @staticmethod
    def is_valid_type(type):
        return type == ClusterType.EC2 or \
            type == ClusterType.TRADITIONAL or \
            type == ClusterType.ANSIBLE or \
            type == ClusterType.NEWT


class VolumeType:
    EBS = 'ebs'

    @staticmethod
    def is_valid_type(type):
        return type == VolumeType.EBS


class VolumeState:
    PROVISIONING = 'provisioning'
    AVAILABLE = 'available'
    ATTACHING = 'attaching'
    DETACHING = 'detaching'
    INUSE = 'in-use'
    DELETING = 'deleting'
    ERROR = 'error'


class QueueType:
    SGE = 'sge'
    PBS = 'pbs'
    SLURM = 'slurm'
    NEWT = 'newt'


class JobQueueState:
    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETE = 'complete'
    ERROR = 'error'


class ClusterStatus(object):
    ERROR = "error"
    CREATED = "created"
    LAUNCHING = "launching"
    PROVISIONING = "provisioning"
    RUNNING = "running"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    STOPPING = "stopping"
    STOPPED = "stopped"
    STARTING = "starting"

    valid_transitions = {}

    def __init__(self, cluster_adapter):
        self.cluster_adapter = cluster_adapter

    def to(self, new_status):
        if self.validate(self.status, new_status):
            self.status = new_status
        else:
            raise Exception(
                "Cannot transition from state \"%s\" to state \"%s\"" %
                (self.status, new_status))

    @classmethod
    def validate(cls, frm, to):
        assert frm in cls.valid_transitions.keys(), \
            u"%s is not a valid ClusterStatus." % frm

        assert to in cls.valid_transitions.keys(), \
            u"%s is not a valid ClusterStatus." % to

        return to in cls.valid_transitions[frm]

    @property
    def nodes(self):
        return self.valid_transitions.keys()

    @property
    def status(self):
        return self.cluster_adapter.cluster['status']

    @status.setter
    def status(self, new_status):
        assert new_status in self.nodes, \
            u"%s is not a valid ClusterStatus." % new_status

        self.cluster_adapater.cluster['status'] = new_status

    def __str__(self):
        return self.status


ClusterStatus.valid_transitions = {
    # Traditional clusters can move directly into RUNNING
    ClusterStatus.CREATED: [ClusterStatus.LAUNCHING,
                            ClusterStatus.RUNNING,
                            ClusterStatus.ERROR],
    ClusterStatus.LAUNCHING: [ClusterStatus.RUNNING,
                              ClusterStatus.ERROR],
    ClusterStatus.PROVISIONING: [ClusterStatus.RUNNING,
                                 ClusterStatus.ERROR],
    ClusterStatus.RUNNING: [ClusterStatus.TERMINATING,
                            ClusterStatus.STOPPING,
                            ClusterStatus.ERROR],
    ClusterStatus.TERMINATING: [ClusterStatus.TERMINATED,
                                ClusterStatus.ERROR],
    ClusterStatus.STOPPING: [ClusterStatus.STOPPED,
                             ClusterStatus.ERROR],
    ClusterStatus.STOPPED: [ClusterStatus.STARTING,
                            ClusterStatus.ERROR],
    ClusterStatus.STARTING: [ClusterStatus.RUNNING,
                             ClusterStatus.ERROR],
    ClusterStatus.TERMINATED: [],
    ClusterStatus.ERROR: []
}


class JobState:
    CREATED = 'created'
    RUNNING = 'running'
    TERMINATED = 'terminated'
    TERMINATING = 'terminating'
    UNEXPECTEDERROR = 'unexpectederror'
    QUEUED = 'queued'
    ERROR = 'error'
    UPLOADING = 'uploading'
    ERROR_UPLOADING = 'error_uploading',
    COMPLETE = 'complete'
