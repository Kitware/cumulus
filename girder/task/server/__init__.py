import sys

from .task import Task

def load(info):
    info['apiRoot'].tasks = Task()
