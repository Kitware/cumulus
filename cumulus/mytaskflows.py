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

from __future__ import absolute_import

import time
from cumulus import taskflow

from celery import chord

class SimpleTaskFlow(taskflow.TaskFlow):
    """
    This is a simple linear taskflow, chain together 6 task. Notice that
    simple_task3 "fans" out the flow, by scheduling 10 copies of simple_task4.
    """
    def start(self):
        simple_task1.delay(self)

    def terminate(self):
        simple_terminate.delay(self)

@taskflow.task
def simple_terminate(workflow, *args, **kwargs):
    workflow.logger.info('Terminating flow')
    time.sleep(3)

@taskflow.task
def simple_task1(workflow, *args, **kwargs):
    workflow.logger.info('Starting simple_task1')
    workflow.set('test', {'nested': 'value'})
    print ('simple_task1')
    simple_task2.delay()

@taskflow.task
def simple_task2(workflow, *args, **kwargs):
    print ('simple_task2')
    time.sleep(3)
    workflow.set('test', {'nested2': 'value'})
    simple_task3.delay()

@taskflow.task
def simple_task3(workflow, *args, **kwargs):
    print ('simple_task3')

    for i in range(0, 10):
        simple_task4.delay()

@taskflow.task
def simple_task4(workflow, *args, **kwargs):
    print ('simple_task4')
    time.sleep(2)

    simple_task5.delay()

@taskflow.task
def simple_task5(workflow, *args, **kwargs):
    print ('simple_task5')
    simple_task6.delay()

@taskflow.task
def simple_task6(workflow, *args, **kwargs):
    print ('simple_task6 and done')


class ChordTaskFlow(taskflow.TaskFlow):
    """
    This taskflow has a "fanout" an also a chord, at task4
    """
    def start(self):
        task1.delay(self)

@taskflow.task
def task1(workflow, *args, **kwargs):
    print ('task1')
    task2.delay()

@taskflow.task
def task2(workflow, *args, **kwargs):
    print ('task2')
    time.sleep(3)

    task3.delay()

@taskflow.task
def task3(workflow, *args, **kwargs):
    print ('task3')

    for i in range(0, 10):
        task4.delay()

@taskflow.task
def task4(workflow, *args, **kwargs):
    print ('task4')
    time.sleep(2)

    header = [task5.s() for i in range(10)]
    chord(header)(task6.s())

@taskflow.task
def task5(workflow, *args, **kwargs):
    print ('task5')

@taskflow.task
def task6(workflow, chord_result, *args, **kwargs):
    print 'task6 and done'


# Example that connects to sequence of tasks together, to allow reuse of sub
# flows.

@taskflow.task
def part1_start(workflow, *args, **kwargs):
    print ('part1 - task1')
    part1_task2.delay()

@taskflow.task
def part1_task2(workflow, *args, **kwargs):
    print ('part1 - task2')
    time.sleep(3)
    part1_task3.delay()

@taskflow.task
def part1_task3(workflow, *args, **kwargs):
    print ('part1 - task3')


@taskflow.task
def part2_start(workflow, *args, **kwargs):
    print ('part2 - task1')
    part2_task2.delay()

@taskflow.task
def part2_task2(workflow, *args, **kwargs):
    print ('part2 - task2')
    time.sleep(3)
    part2_task3.delay()

@taskflow.task
def part2_task3(workflow, *args, **kwargs):
    print ('part2 - task3')
    time.sleep(3)

@taskflow.task
def part3_start(workflow, *args, **kwargs):
    print ('part3 - start')
    time.sleep(3)

class Part1TaskFlow(taskflow.TaskFlow):
    def start(self):
        part1_start.delay(self)

class Part2TaskFlow(taskflow.TaskFlow):
    def start(self):
        part2_start.delay(self)

class Part3TaskFlow(taskflow.TaskFlow):
    def start(self):
        part3_start.delay(self)



# This syntax is a little messy I think. It would be nice not the have to create
# this extra taskflow class, we need it at the moment to support the creation of
# the taskflow via the REST endpoint.
class MyCompositeTaskFlow(taskflow.CompositeTaskFlow):

    def __init__(self, *args, **kwargs):
        super(MyCompositeTaskFlow, self).__init__(*args, **kwargs)

        self.add(Part1TaskFlow(*args, **kwargs))
        self.add(Part2TaskFlow(*args, **kwargs))
        self.add(Part3TaskFlow(*args, **kwargs))



class ConnectTwoTaskFlow(taskflow.TaskFlow):
    """
    This taskflow connects two sequences of task together.
    """
    def __init__(self, *args, **kwargs):
        super(ConnectTwoTaskFlow, self).__init__(*args, **kwargs)

        # This is where we make the connection
        # when part1_task3 is complete run part2_start
        # Not sure about the syntax?
        self.on_complete(part1_task3).run(part2_start.s())

    def start(self):
        part1_start.delay(self)

