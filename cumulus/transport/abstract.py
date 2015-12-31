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


class AbstractConnection(object):

    def execute(self, command, ignore_exit_status=False, source_profile=True):
        raise NotImplementedError('Implemented by subclass')

    def get(self, remote_path):
        raise NotImplementedError('Implemented by subclass')

    def isfile(self, remote_path):
        raise NotImplementedError('Implemented by subclass')

    def mkdir(self, path, ignore_failure=False):
        raise NotImplementedError('Implemented by subclass')

    def makedirs(self, path):
        raise NotImplementedError('Implemented by subclass')

    def put(self, stream, remote_path):
        raise NotImplementedError('Implemented by subclass')

    def stat(self):
        raise NotImplementedError('Implemented by subclass')

    def remove(self, remote_path):
        raise NotImplementedError('Implemented by subclass')

    def list(self, remove_path):
        """
        Returns an array of objects of the form:

        {
            'name': <name> ,
            'group': <group>,
            'user': <user>,
            'mode': <mode>,
            'date': <data>,
            'size': <size>
        }
        """
        raise NotImplementedError('Implemented by subclass')
