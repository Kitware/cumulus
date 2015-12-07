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
    AVAILABLE = 'available'
    INUSE = 'in-use'


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


class ClusterStatus(IntEnum):
    error = -1
    creating = 0
    created = 10
    launching = 20
    launched = 30
    provisioning = 40
    provisioned = 50
    terminating = 60
    terminated = 70
    stopped = 101
    running = 102

    def __str__(self):
        return '"' + self.name + '"'
