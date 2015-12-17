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

from jsonpath_rw import parse

from . import sge
from . import pbs
from cumulus.constants import QueueType

type_to_adapter = {
    QueueType.SGE: sge.SgeQueueAdapter,
    QueueType.PBS: pbs.PbsQueueAdapter
}


def get_queue_adapter(cluster, cluster_connection=None):
    global type_to_adapter

    system = parse('config.scheduler.type').find(cluster)
    if system:
        system = system[0].value
    # Default to SGE
    else:
        system = QueueType.SGE

    if system not in type_to_adapter:
        raise Exception('Unsupported queuing system: %s' % system)

    return type_to_adapter[system](cluster, cluster_connection)
