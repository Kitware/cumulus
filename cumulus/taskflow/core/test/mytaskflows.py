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
from cumulus.celery import command

from celery import chord


class SimpleTaskFlow(taskflow.TaskFlow):
    """
    This is a simple linear taskflow, chain together 6 task. Notice that
    simple_task3 "fans" out the flow, by scheduling 10 copies of simple_task4.
    """
    def start(self, *args, **kwargs):
        # Prove that we can still run a regular celery task
        regular_task.delay()

        # Run simple task workflow
        super(SimpleTaskFlow, self).start(simple_task1.s(*args, **kwargs))

    def terminate(self, *args, **kwargs):
        super(SimpleTaskFlow, self).start(simple_terminate.s(*args, **kwargs))

    def delete(self, *args, **kwargs):
        super(SimpleTaskFlow, self).start(simple_delete.s(*args, **kwargs))


class LinkTaskFlow(taskflow.TaskFlow):
    """
    This is a simple linear taskflow, chain together 6 task. Notice that
    simple_task3 "fans" out the flow, by scheduling 10 copies of simple_task4.
    """
    def start(self, *args, **kwargs):
        # Prove that we can still run a regular celery task
        super(LinkTaskFlow, self).start(regular_task.s(),
                                        link=simple_task1.s())

    def terminate(self, *args, **kwargs):
        super(LinkTaskFlow, self).start(simple_terminate.s(*args, **kwargs))

    def delete(self, *args, **kwargs):
        super(LinkTaskFlow, self).start(simple_delete.s(*args, **kwargs))


@command.task
def regular_task():
    'regular_task!!'
    time.sleep(3)


@taskflow.task
def simple_terminate(task, *args, **kwargs):
    task.taskflow.logger.info('Terminating flow')
    time.sleep(3)


@taskflow.task
def simple_delete(task, *args, **kwargs):
    task.taskflow.logger.info('Deleting flow')
    time.sleep(3)


@taskflow.task
def simple_task1(task, *args, **kwargs):
    task.taskflow.logger.info('Starting simple_task1')
    task.logger.info('Task level logging')
    task.taskflow.set_metadata('test', {'nested': 'value'})
    task.taskflow.logger.debug('simple_task1')
    simple_task2.delay()


@taskflow.task
def simple_task2(task, *args, **kwargs):
    task.taskflow.logger.debug('simple_task2')
    time.sleep(3)
    task.taskflow.set_metadata('test', {'nested2': 'value'})
    simple_task3.delay()


@taskflow.task
def simple_task3(task, *args, **kwargs):
    task.taskflow.logger.debug('simple_task3')
    for i in range(0, 10):
        simple_task4.delay()


@taskflow.task
def simple_task4(task, *args, **kwargs):
    task.taskflow.logger.debug('simple_task4')
    time.sleep(2)

    simple_task5.delay()


@taskflow.task
def simple_task5(task, *args, **kwargs):
    task.taskflow.logger.debug('simple_task5')
    simple_task6.delay()


@taskflow.task
def simple_task6(task, *args, **kwargs):
    task.taskflow.logger.debug('simple_task6 and done')


class ChordTaskFlow(taskflow.TaskFlow):
    """
    This taskflow has a "fanout" an also a chord, at task4
    """
    def start(self, *args, **kwargs):
                # Run simple task workflow
        super(ChordTaskFlow, self).start(task1.s(*args, **kwargs))


@taskflow.task
def task1(task, *args, **kwargs):
    task.taskflow.logger.debug('task1')
    task2.delay()


@taskflow.task
def task2(task, *args, **kwargs):
    task.taskflow.logger.debug('task2')
    time.sleep(3)

    task3.delay()


@taskflow.task
def task3(task, *args, **kwargs):
    task.taskflow.logger.debug('task3')

    for i in range(0, 10):
        task4.delay()


@taskflow.task
def task4(task, *args, **kwargs):
    task.taskflow.logger.debug('task4')
    time.sleep(2)

    header = [task5.s() for i in range(10)]

    chord(header)(task6.s())


@taskflow.task
def task5(task, *args, **kwargs):
    task.taskflow.logger.debug('task5')


@taskflow.task
def task6(task, chord_result, *args, **kwargs):
    task.taskflow.logger.debug('task6 and done')


# Example that connects to sequence of tasks together, to allow reuse of sub
# flows.


@taskflow.task
def part1_start(task, *args, **kwargs):
    task.taskflow.logger.debug('part1 - task1')
    part1_task2.delay()


@taskflow.task
def part1_task2(task, *args, **kwargs):
    task.taskflow.logger.debug('part1 - task2')
    time.sleep(3)
    part1_task3.delay()


@taskflow.task
def part1_task3(task, *args, **kwargs):
    task.taskflow.logger.debug('part1 - task3')


@taskflow.task
def part2_start(task, *args, **kwargs):
    task.taskflow.logger.debug('part2 - task1')
    part2_task2.delay()


@taskflow.task
def part2_task2(task, *args, **kwargs):
    task.taskflow.logger.debug('part2 - task2')
    time.sleep(3)
    part2_task3.delay()


@taskflow.task
def part2_task3(task, *args, **kwargs):
    task.taskflow.logger.debug('part2 - task3')
    time.sleep(3)


@taskflow.task
def part3_start(task, *args, **kwargs):
    task.taskflow.logger.debug('part3 - start')
    time.sleep(3)


class Part1TaskFlow(taskflow.TaskFlow):
    def start(self, *args, **kwargs):
        # Run simple task workflow
        super(Part1TaskFlow, self).start(part1_start.s(*args, **kwargs))


class Part2TaskFlow(taskflow.TaskFlow):
    def start(self, *args, **kwargs):
        # Run simple task workflow
        super(Part2TaskFlow, self).start(part2_start.s(*args, **kwargs))


class Part3TaskFlow(taskflow.TaskFlow):
    def start(self, *args, **kwargs):
        super(Part3TaskFlow, self).start(part3_start.s(*args, **kwargs))


# This syntax is a little messy I think. It would be nice not the have to create
# this extra taskflow class, we need it at the moment to support the creation of
# the taskflow via the REST endpoint.
# class MyCompositeTaskFlow(taskflow.CompositeTaskFlow):
#
#     def __init__(self, *args, **kwargs):
#         super(MyCompositeTaskFlow, self).__init__(*args, **kwargs)
#
#         self.add(Part1TaskFlow(*args, **kwargs))
#         self.add(Part2TaskFlow(*args, **kwargs))
#         self.add(Part3TaskFlow(*args, **kwargs))


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

    def start(self, *args, **kwargs):
        super(ConnectTwoTaskFlow, self).start(part1_start.s(*args, **kwargs))
