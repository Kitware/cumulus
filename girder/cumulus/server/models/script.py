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

from girder.constants import AccessType
from .base import BaseModel


class Script(BaseModel):

    def __init__(self):
        super(Script, self).__init__()

    def initialize(self):
        self.name = 'scripts'

    def validate(self, doc):
        return doc

    def create(self, user, script):

        doc = self.setUserAccess(script, user, level=AccessType.ADMIN,
                                 save=True)

        return doc
