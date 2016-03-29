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


from .ssh import SshClusterConnection
from .newt import NewtClusterConnection

ssh_cluster = ['trad', 'ec2']


def get_connection(girder_token, cluster):
    if cluster['type'] in ssh_cluster:
        return SshClusterConnection(girder_token, cluster)
    elif cluster['type'] == 'newt':
        return NewtClusterConnection(girder_token, cluster)
    else:
        raise Exception('Unsupported cluster type: %s' % cluster['type'])
