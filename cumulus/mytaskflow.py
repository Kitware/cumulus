from __future__ import absolute_import

import time

from cumulus import taskflow
from functools import wraps
from celery import group

class MyTaskFlow(taskflow.TaskFlow):
    # State methods
    def start(self):
        task1.delay(self)

# tasks

@taskflow.task
def task1(workflow, *args, **kwargs):
    print 'task1'
    print workflow
    task2.delay(workflow)

@taskflow.task
def task2(workflow, *args, **kwargs):
    print 'task2'

    #header = [add.s(i, i) for i in range(100)]

    #>>> result = chord(header)(callback)
    #for i in range(0, 10):

    #group(task3.s(workflow)).delay()
    time.sleep(3)

    task3.delay(workflow)

@taskflow.task
def task3(workflow, *args, **kwargs):
    print 'task3'

    #header = [task5.s(workflow) for i in range(10)]
    #taskflow.chord(header)(task6.s(workflow))

    task4.delay(workflow)

@taskflow.task
def task4(workflow, *args, **kwargs):
    #raise Exception('task4')
    print 'task5'
    time.sleep(2)
#     pass


@taskflow.task
def task5(workflow, *args, **kwargs):
    print 'task5'

@taskflow.task
def task6(chord_result, workflow, *args, **kwargs):
    print 'task6 and done'


