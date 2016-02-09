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
import os
import pkgutil
import pkg_resources as pr
import sys

import cumulus


def find_modules(paths, prefix=''):
    for (loader, name, pkg) in pkgutil.iter_modules(paths):
        if pkg:
            package_dir = os.path.join(loader.path, name)
            for module in find_modules([package_dir], prefix='%s%s.' % (prefix,
                                                                        name)):
                yield module
        else:
            yield '%s%s' % (prefix,  name)


def find_taskflow_modules(prefix=''):
    modules = []
    # First add core modules
    base_path = os.path.abspath(
                            os.path.dirname(
                                pr.resource_filename(cumulus.__name__,
                                                     '__init__.py')))
    modules += find_modules([os.path.join(base_path, 'taskflow', 'core')],
                            prefix='cumulus.taskflow.core.')

    if 'taskflow' in cumulus.config:
        modules += find_modules(cumulus.config.taskflow.path)

    return modules
