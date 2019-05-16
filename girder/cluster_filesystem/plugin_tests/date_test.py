#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2018 Kitware Inc.
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


import pytest
import mock
import datetime as dt

from . import unbound_server

@pytest.mark.plugin('cluster_filesystem')
@mock.patch('cluster_filesystem.Cluster')
def test_date_parser(cluster, unbound_server):
    from cluster_filesystem.server.dateutils import date_parser
    # pass a string in the format:
    # May  2 09:06
    # assumes it is within 6 months from now
    now = dt.datetime.now()
    now = dt.datetime(
        year=now.year, month=now.month, day=now.day,
        hour=now.hour, minute=now.minute, second=0,
        microsecond=0
    )
    timestamp = dt.datetime.strftime(now, "%b %d %H:%M")
    assert now.isoformat() == date_parser(timestamp)

    # pass a string in the format:
    # May  2 2018
    # assumes it is further tha 6 months from now
    now = dt.datetime.now()
    now = dt.datetime(
        year=now.year, month=now.month, day=now.day,
        hour=0, minute=0, second=0,
        microsecond=0
    )
    now += dt.timedelta(days=200)
    timestamp = dt.datetime.strftime(now, "%b %d %Y")
    assert now.isoformat() == date_parser(timestamp)

    now -= dt.timedelta(days=400)
    timestamp = dt.datetime.strftime(now, "%b %d %Y")
    assert now.isoformat() == date_parser(timestamp)

    # pass any other string
    timestamp = "Some other weird date format"
    assert timestamp == date_parser(timestamp)

