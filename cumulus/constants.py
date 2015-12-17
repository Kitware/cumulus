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


class ClusterType:
    EC2 = 'ec2'
    TRADITIONAL = 'trad'

    @staticmethod
    def is_valid_type(type):
        return type == ClusterType.EC2 or type == ClusterType.TRADITIONAL


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

    @staticmethod
    def is_valid_type(type):
        return type == QueueType.SGE or type == QueueType.PBS


class JobQueueState:
    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETE = 'complete'
    ERROR = 'error'
