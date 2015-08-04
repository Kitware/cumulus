from __future__ import absolute_import
import logging
import sys
import contextlib
import requests
import json
import threading
import starcluster.logger
import functools
import uuid
import traceback


class StarClusterLogHandler(logging.Handler):

    def __init__(self, token, url, level=logging.NOTSET):
        super(StarClusterLogHandler, self).__init__(level)
        self._url = url
        self._headers = {'Girder-Token':  token}

    def emit(self, record):
        json_str = json.dumps(record.__dict__, default=str)
        print >> sys.stderr,  json_str
        print >> sys.stderr,  self._url
        r = None
        try:
            r = requests.post(self._url, headers=self._headers, data=json_str)
            print >> sys.stderr, self._url
            r.raise_for_status()
        except Exception:
            if r:
                print >> sys.stderr, 'Unable to POST log record: %s' % r.content
            else:
                print >> sys.stderr, traceback.format_exc()


@contextlib.contextmanager
def logstdout():
    old_stdout = sys.stdout

    try:
        sys.stdout = StarClusterCallWriteHandler()
        yield
    finally:
        sys.stdout = old_stdout


def capture(func):
    @functools.wraps(func)
    def captureDecorator(self, *args, **kwargs):
        logger = starcluster.logger.get_starcluster_logger()
        logger.setLevel(logging.INFO)
        handler = StarClusterLogHandler(kwargs['girder_token'],
                                        kwargs['log_write_url'], logging.DEBUG)
        logger.addHandler(handler)
        call_id = uuid.uuid4()
        threadlocal = threading.local()
        threadlocal.id = call_id
        handler.addFilter(StarClusterLogFilter(threadlocal, call_id))

        try:
            r = func(self, *args, **kwargs)
        finally:
            del threadlocal.id
            logger.removeHandler(handler)

        return r

    return captureDecorator


class StarClusterCallWriteHandler:

    def __init__(self):
        self._logger = starcluster.logger.get_starcluster_logger()

    def write(self, value):
        if value != '\n':
            self._logger.info(value)

    def flush(self):
        # Do nothing for now
        pass


class StarClusterLogFilter():

    def __init__(self, threadlocal, id):
        self._id = id
        self._threadlocal = threadlocal

    def filter(self, record):
        call_id = getattr(self._threadlocal, 'id', None)

        return call_id != id
