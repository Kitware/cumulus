from __future__ import absolute_import
import logging
import sys
import contextlib
import requests
import json

import starcluster.logger

class StarClusterLogHandler(logging.Handler):
    def __init__(self, url, level=logging.NOTSET):
        super(StarClusterLogHandler, self).__init__(level)
        self._url = url

    def emit(self, record):
        json_str = json.dumps(record, default=lambda obj: obj.__dict__)
        print >> sys.stderr,  json_str
        print >> sys.stderr,  self._url
        r = requests.post(self._url, data=json_str)
        r.raise_for_status()

@contextlib.contextmanager
def logstdout():
    old_stdout = sys.stdout

    try:
        sys.stdout = StarClusterCallWriteHandler()
        yield
    finally:
        sys.stdout = old_stdout

class StarClusterCallWriteHandler:
    def __init__(self):
        self._logger = starcluster.logger.get_starcluster_logger()

    def write(self, value):
        if value != '\n':
            self._logger.info(value)

