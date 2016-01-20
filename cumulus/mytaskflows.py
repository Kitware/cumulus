from __future__ import absolute_import

import time
from cumulus import taskflow

from celery import chord

class SimpleTaskFlow(taskflow.TaskFlow):
    def start(self):
        simple_task1.delay(self)

@taskflow.task
def simple_task1(workflow, *args, **kwargs):
    print 'simple_task1'
    simple_task2.delay()

@taskflow.task
def simple_task2(workflow, *args, **kwargs):
    print 'simple_task2'
    time.sleep(3)

    simple_task3.delay()

@taskflow.task
def simple_task3(workflow, *args, **kwargs):
    print 'simple_task3'

    for i in range(0, 10):
        simple_task4.delay()

@taskflow.task
def simple_task4(workflow, *args, **kwargs):
    #raise Exception('task4')
    print 'simple_task4'
    time.sleep(2)

    simple_task5.delay()

@taskflow.task
def simple_task5(workflow, *args, **kwargs):
    print 'simple_task5'
    simple_task6.delay()

@taskflow.task
def simple_task6(workflow, *args, **kwargs):
    print 'simple_task6 and done'


class ChordTaskFlow(taskflow.TaskFlow):
    # State methods
    def start(self):
        task1.delay(self)

@taskflow.task
def task1(workflow, *args, **kwargs):
    print 'task1'
    print workflow
    task2.delay()

@taskflow.task
def task2(workflow, *args, **kwargs):
    print 'task2'
    time.sleep(3)

    task3.delay()

@taskflow.task
def task3(workflow, *args, **kwargs):
    print 'task3'

    for i in range(0, 10):
        task4.delay()

@taskflow.task
def task4(workflow, *args, **kwargs):
    #raise Exception('task4')
    print 'task4'
    time.sleep(2)

    header = [task5.s() for i in range(10)]
    chord(header)(task6.s())

@taskflow.task
def task5(workflow, *args, **kwargs):
    print 'task5'

@taskflow.task
def task6(workflow, chord_result, *args, **kwargs):
    print 'task6 and done'


