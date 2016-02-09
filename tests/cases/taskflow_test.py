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

import unittest
import os
import tempfile
import shutil
import contextlib

import cumulus
from cumulus.taskflow.utility import find_modules

class TaskFlowTestCase(unittest.TestCase):

    @contextlib.contextmanager
    def _make_temp_directory(self):
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_load_tasks(self):
        # Create a test directory structure
        with self._make_temp_directory() as module_dir1, \
            self._make_temp_directory() as module_dir2, \
            self._make_temp_directory() as module_dir3:

            paths = [
                os.path.join(module_dir1, '__init__.py'),
                os.path.join(module_dir1, 'task1.py'),
                os.path.join(module_dir2, 'task2.py'),
                os.path.join(module_dir3, 'task3.py'),
                os.path.join(module_dir3, 'package/task4.py'),
                os.path.join(module_dir3, 'package/__init__.py'),
                os.path.join(module_dir3, 'package/subpackage/__init__.py'),
                os.path.join(module_dir3, 'package/subpackage/secret_task.py')
            ]

            for path in paths:
                try:
                    os.makedirs(os.path.dirname(path))
                except:
                    pass
                open(path, 'w').close()

            expected = set([
                'task1',
                'task2',
                'task3',
                'package.task4',
                'package.subpackage.secret_task'
            ])

            modules = find_modules([module_dir1, module_dir2, module_dir3])
            self.assertEquals(set(modules), expected)





