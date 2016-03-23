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

from __future__ import absolute_import
import logging
import sys
import requests
import json
import traceback


class StarClusterLogHandler(logging.Handler):

    def __init__(self, token, url, level=logging.NOTSET):
        super(StarClusterLogHandler, self).__init__(level)
        self._url = url
        self._headers = {'Girder-Token':  token}

    def emit(self, record):
        json_str = json.dumps(record.__dict__, default=str)
        r = None
        try:
            r = requests.post(self._url, headers=self._headers, data=json_str)
            r.raise_for_status()
        except Exception:
            if r:
                print >> sys.stderr, 'Unable to POST log record: %s' % r.content
            else:
                print >> sys.stderr, traceback.format_exc()
