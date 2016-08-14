#!/usr/bin/env python2.7
from __future__ import print_function

import site
site.addsitedir('/usr/lib/python2.7/site-packages')
site.addsitedir('/usr/local/lib/python2.7/site-packages')

import sys
import time
from threading import Thread

from mesos.interface import Executor, mesos_pb2
from mesos.native import MesosExecutorDriver


class MinimalExecutor(Executor):
    def launchTask(self, driver, task):
        def run_task():
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            print(task.data)
            time.sleep(30)

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            driver.sendStatusUpdate(update)

        thread = Thread(target=run_task, args=())
        thread.start()


if __name__ == '__main__':
    driver = MesosExecutorDriver(MinimalExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)