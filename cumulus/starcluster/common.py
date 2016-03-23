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
import starcluster.logger
import traceback
import logging
from cumulus.starcluster.logging import StarClusterLogHandler


def _log_exception(ex):
    log = starcluster.logger.get_starcluster_logger()
    log.error(traceback.format_exc())


def get_post_logger(name, girder_token, post_url):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = StarClusterLogHandler(girder_token, post_url, logging.DEBUG)
    logger.addHandler(handler)

    return logger
