#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2016 Kitware Inc.
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

import json
import re


class AssertCallsMixin(object):
    """
    This mixin add support for asserting mock call_args_lists.
    """

    def normalize(self, data):
        str_data = json.dumps(data, default=str)
        str_data = re.sub(r'[\w]{64}', 'token', str_data)

        return json.loads(str_data)

    def assertArgs(self, actual, expected):
        self.assertEqual(len(actual), len(expected),
                         'Number of args do not match: %d != %d' %
                         (len(actual), len(expected)))
        for i in range(0, len(actual)):
            try:
                self.assertEqual(actual[i], expected[i])
            except AssertionError as err:
                msg = 'arg at index %d does not match:\n%s' % (i, err)
                err.args = (msg,)
                raise err

    def assertCall(self, actual, expected):
        (actual_args, actual_kwargs) = actual
        (expected_args, expected_kwargs) = expected

        self.assertArgs(actual_args, expected_args)
        self.assertEqual(actual_kwargs, expected_kwargs)

    def assertCalls(self, actual, expected):
        calls = self.normalize(actual)

        self.assertEqual(len(calls), len(expected),
                         'Number of calls do not match: %d != %d' %
                         (len(calls), len(expected)))

        for i in range(0, len(calls)):
            try:
                self.assertCall(calls[i], expected[i])
            except AssertionError as err:
                msg = 'Call at index %d does not match:\n%s' % (i, err)
                err.args = (msg,)
                raise err
