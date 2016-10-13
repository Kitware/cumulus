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

from setuptools import setup, find_packages
import os
import re

def get_data_files(path, include=None, exclude=None):
    """Recursively produce a list appropriate for setup's data_files option.

    :param path: Path to descend
    :param include: white list of regular expressions (can include directories)
    :param exclude: black list of regular expressions (can include directories)
    :returns: list of tuples [(directory, [file, file, ...]), ...]
    :rtype: list

    """
    include = re.compile("|".join(include) if include is not None else ".*")

    if exclude is not None:
        exclude = re.compile("|".join(exclude))

    for directory, subdirectories, files in os.walk(path):
        filtered = [f for f in [os.path.join(directory, f) for f in files]
                    if (include.match(f) and
                        (exclude is None or not exclude.match(f)))]

        if filtered:
            yield (directory, filtered)

version_py = os.path.join(os.path.dirname(__file__), 'cumulus', 'version.py')
with open(version_py) as fd:
    exec(fd.read())

setup(
    name='cumulus',
    version=__version__,
    description='A RESTful API for the creation & management of HPC clusters',
    author='Kitware',
    author_email='cumulus@kitware.com',
    url='https://github.com/Kitware/cumulus',
    packages=find_packages(exclude=['*.tests', '*.tests.*',
                                    'tests.*', 'tests']),
    package_data={
        '': ['*.json', '*.sh'],
        'cumulus': ['conf/*.json',
                    'templates/*.sh',
                    'templates/*/*.sh']
    },
    entry_points = {
        'console_scripts': ['cumulus=cumulus.scripts:cli']
    },

    data_files=list(get_data_files('cumulus/ansible/tasks/playbooks/',
                                   exclude=['.*pyc$']))
)
