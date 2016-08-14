#!/usr/bin/env python2.7
from __future__ import print_function

import site
site.addsitedir('/usr/lib/python2.7/site-packages')
site.addsitedir('/usr/local/lib/python2.7/site-packages')

import os
import sys
import uuid
import time
import signal
from threading import Thread

from mesos.interface import Scheduler, mesos_pb2
from mesos.native import MesosSchedulerDriver


class MinimalScheduler(Scheduler):
    def __init__(self, executor):
        self.executor = executor

    def resourceOffers(self, driver, offers):
        """
          Invoked when resources have been offered to this framework. A single
          offer will only contain resources from a single slave.  Resources
          associated with an offer will not be re-offered to _this_ framework
          until either (a) this framework has rejected those resources (see
          SchedulerDriver.launchTasks) or (b) those resources have been
          rescinded (see Scheduler.offerRescinded).  Note that resources may be
          concurrently offered to more than one framework at a time (depending
          on the allocator being used).  In that case, the first framework to
          launch tasks using those resources will be able to use them while the
          other frameworks will have those resources rescinded (or if a
          framework has already launched tasks with those resources then those
          tasks will fail with a TASK_LOST status and a message saying as much).
        """
        for offer in offers:
            task = mesos_pb2.TaskInfo()
            task_id = str(uuid.uuid4())
            task.task_id.value = task_id
            task.slave_id.value = offer.slave_id.value
            task.name = "task {}".format(task_id)
            task.executor.MergeFrom(self.executor)
            task.data = "Hello from task {}!".format(task_id)

            cpus = task.resources.add()
            cpus.name = 'cpus'
            cpus.type = mesos_pb2.Value.SCALAR
            cpus.scalar.value = 0.1

            mem = task.resources.add()
            mem.name = 'mem'
            mem.type = mesos_pb2.Value.SCALAR
            mem.scalar.value = 32

            tasks = [task]
            driver.launchTasks(offer.id, tasks)


def main(master):
    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = 'MinimalExecutor'
    executor.name = executor.executor_id.value
    executor.command.value = os.path.abspath('./executor.py')

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ''
    framework.name = "MinimalFramework"
    framework.checkpoint = True
    framework.principal = framework.name

    implicitAcknowledgements = 1

    driver = MesosSchedulerDriver(
        MinimalScheduler(executor),
        framework,
        master,
        implicitAcknowledgements
    )

    def signal_handler(signal, frame):
        driver.stop()

    def run_driver_thread():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    print('Scheduler running, Ctrl+C to quit.')
    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)

    sys.exit(0)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: {} <mesos_master>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1])
