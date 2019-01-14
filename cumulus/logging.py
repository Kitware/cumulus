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
import types


class LogRecordEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, type):
            return obj.__name__
        elif isinstance(obj, types.TracebackType):
            return traceback.format_tb(obj)
        else:
            return str(obj)


class RESTfulLogHandler(logging.Handler):

    def __init__(self, girder_token, url, level=logging.NOTSET):
        super(RESTfulLogHandler, self).__init__(level)
        self._url = url
        self._headers = {'Girder-Token':  girder_token}

    def emit(self, record):
        json_str = json.dumps(record.__dict__, cls=LogRecordEncoder)
        r = None
        try:
            r = requests.post(self._url, headers=self._headers, data=json_str)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                sys.stderr.write(
                    'Logging endpoint appears to have disappeared.\n')
            else:
                raise e
        except Exception:
            traceback.print_stack()
            if r:
                sys.stderr.write('Unable to POST log record: %s\n' % r.content)
