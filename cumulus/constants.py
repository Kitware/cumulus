#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2015 Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the 'License' );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an 'AS IS' BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################


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
    CREATED = 'created'
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


@six.python_2_unicode_compatible
class ClusterStatus(object):
    ERROR = 'error'
    CREATING = 'creating'
    CREATED = 'created'
    LAUNCHING = 'launching'
    PROVISIONING = 'provisioning'
    RUNNING = 'running'
    TERMINATING = 'terminating'
    TERMINATED = 'terminated'
    STOPPING = 'stopping'
    STOPPED = 'stopped'
    STARTING = 'starting'

    valid_transitions = {}

    def __init__(self, cluster_adapter):
        self.cluster_adapter = cluster_adapter

    def to(self, new_status, e=None):
        e = Exception('Cannot transition cluster from %s to %s.'
                      % (self.status, new_status)) if e is None else e
        try:
            if self.valid_transition(self.status, new_status):
                self.status = new_status
            else:
                raise e
        except Exception:
            raise e

    @classmethod
    def valid(cls, status):
        return status in cls.valid_transitions.keys()

    @classmethod
    def valid_transition(cls, frm, to):
        if not cls.valid(frm):
            raise Exception(u'%s is not a valid ClusterStatus.' % frm)

        if not cls.valid(to):
            raise Exception(u'%s is not a valid ClusterStatus.' % to)

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
            u'%s is not a valid ClusterStatus.' % new_status

        self.cluster_adapter.cluster['status'] = new_status

    def __str__(self):
        return self.status


ClusterStatus.valid_transitions = {
    # Traditional clusters can move directly into RUNNING
    ClusterStatus.CREATING: [ClusterStatus.CREATING,
                             ClusterStatus.CREATED,
                             ClusterStatus.ERROR],
    ClusterStatus.CREATED: [ClusterStatus.CREATED,
                            ClusterStatus.LAUNCHING,
                            ClusterStatus.RUNNING,
                            ClusterStatus.ERROR],
    ClusterStatus.LAUNCHING: [ClusterStatus.LAUNCHING,
                              ClusterStatus.RUNNING,
                              ClusterStatus.ERROR],
    ClusterStatus.PROVISIONING: [ClusterStatus.PROVISIONING,
                                 ClusterStatus.RUNNING,
                                 ClusterStatus.ERROR],
    ClusterStatus.RUNNING: [ClusterStatus.RUNNING,
                            ClusterStatus.PROVISIONING,
                            ClusterStatus.TERMINATING,
                            ClusterStatus.STOPPING,
                            ClusterStatus.ERROR],
    ClusterStatus.TERMINATING: [ClusterStatus.TERMINATING,
                                ClusterStatus.TERMINATED,
                                ClusterStatus.ERROR],
    ClusterStatus.STOPPING: [ClusterStatus.STOPPING,
                             ClusterStatus.STOPPED,
                             ClusterStatus.ERROR],
    ClusterStatus.STOPPED: [ClusterStatus.STOPPED,
                            ClusterStatus.STARTING,
                            ClusterStatus.ERROR],
    ClusterStatus.STARTING: [ClusterStatus.STARTING,
                             ClusterStatus.RUNNING,
                             ClusterStatus.ERROR],
    ClusterStatus.TERMINATED: [ClusterStatus.TERMINATED],
    ClusterStatus.ERROR: [ClusterStatus.ERROR,
                          ClusterStatus.TERMINATING]
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
