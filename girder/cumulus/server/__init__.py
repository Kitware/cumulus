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

from .cluster import Cluster
from .job import Job
from .script import Script
from .volume import Volume
import aws


def load(info):
    info['apiRoot'].clusters = Cluster()
    info['apiRoot'].jobs = Job()
    info['apiRoot'].scripts = Script()
    info['apiRoot'].volumes = Volume()
    # Augment user resource with aws profiles
    aws.load(info['apiRoot'])
