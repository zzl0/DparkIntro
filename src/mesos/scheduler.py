# coding: utf-8
import sys
import uuid
import time
import pymesos as mesos
from mesos.interface import Scheduler
from mesos.interface import mesos_pb2


import logging
logging.basicConfig(level=logging.DEBUG)

TOTAL_TASKS = 5
TASK_CPUS = 1
TASK_MEM = 1


def new_task(offer):
    task = mesos_pb2.TaskInfo()
    id_ = uuid.uuid4()
    task.task_id.value = str(id_)
    task.slave_id.value = offer.slave_id.value
    task.name = "task {}".format(str(id_))

    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = 1

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = 1

    return task


class HelloWorldScheduler(Scheduler):

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: %s", framework_id)

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: %s", [o.id.value for o in offers])
        # whenever we get an offer, we accept it and use it to launch a task that
        # just echos hello world to stdout
        for offer in offers:
            for resource in offer.resources:
                if resource.name == "cpus":
                    offer_cpus = resource.scalar.value
                elif resource.name == "mem":
                    offer_mem = resource.scalar.value

            if offer_cpus >= 1 and offer_mem >= 1:
                task = new_task(offer)
                task.command.value = "echo hello world"
                time.sleep(2)
                logging.info("Launching task {task} "
                             "using offer {offer}.".format(task=task.task_id.value,
                                                           offer=offer.id.value))
                tasks = [task]
                rf = mesos_pb2.Filters()
                rf.refuse_seconds = 5 * 6
                driver.launchTasks(offer.id, tasks, rf)


def main():
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "zhuzhaolong"
    framework.name = "hello-world"
    driver = mesos.MesosSchedulerDriver(
        HelloWorldScheduler(),
        framework,
        sys.argv[1]
    )
    driver.run()


if __name__ == '__main__':
    main()


# class SimpleScheduler(Scheduler):
#     def __init__(self, executor):
#         self.executor = executor
#         self.task_data = {}
#         self.task_launched = 0
#         self.task_finished = 0
#         self.message_sent = 0
#         self.message_received = 0

#     def registered(self, driver, framework_id, master_info):
#         print 'Registered with framework ID %s' % framework_id.value

#     def resourceOffers(self, driver, offers):
#         for offer in offers:
#             tasks = []
#             offer_cpus = 0
#             offer_mem = 0
#             for resource in offer.resources:
#                 if resource.name == "cpus":
#                     offer_cpus += resource.scalar.value
#                 elif resource.name == "mem":
#                     offer_mem += resource.scalar.value

#             print ("Received offer %s with cpus: %s and mem: %s" %
#                    (offer.id.value, offer_cpus, offer_mem))

#             remaining_cpus = offer_cpus
#             remaining_mem = offer_mem

#             while (self.task_launched < TOTAL_TASKS and
#                    remaining_cpus >= TASK_CPUS and
#                    remaining_mem >= TASK_MEM):
#                 tid = self.task_launched
#                 self.task_launched += 1

#                 print ("Launching task %d using offer %s on %s" %
#                        (tid, offer.id.value, offer.slave_id.value))

#                 # create task

#                 task = mesos_pb2.TaskInfo()
#                 task.task_id.value = str(tid)
#                 task.slave_id.value = offer.slave_id.value
#                 task.name = "task %d" % tid
#                 task.executor.MergeFrom(self.executor)

#                 cpus = task.resources.add()
#                 cpus.name = "cpus"
#                 cpus.type = mesos_pb2.Value.SCALAR
#                 cpus.scalar.value = TASK_CPUS

#                 mem = task.resources.add()
#                 mem.name = "mem"
#                 mem.type = mesos_pb2.Value.SCALAR
#                 mem.scalar.value = TASK_MEM

#                 tasks.append(task)

#                 self.task_data[task.task_id.value] = (
#                     offer.slave_id, task.executor.executor_id
#                 )

#                 remaining_cpus -= TASK_CPUS
#                 remaining_mem -= TASK_MEM

#             rf = mesos_pb2.Filters()
#             rf.refuse_seconds = 5 * 60
#             driver.launchTasks(offer.id, tasks, rf)

#     def statusUpdate(self, driver, update):
#         print ("Task %s is in state %s" %
#                (update.task_id.value, mesos_pb2.TaskState.Name(update.state)))

#         # Ensure the binary data came through.
#         if update.data != "data with a \0 byte":
#             print "The update data did not match!"
#             print "  Expected: 'data with a \\x00 byte'"
#             print "  Actual:  ", repr(str(update.data))

#         if update.state == mesos_pb2.TASK_FINISHED:
#             self.task_finished += 1
#             if self.task_finished == TOTAL_TASKS:
#                 print "All tasks done, waiting for final framework message"

#             slave_id, executor_id = self.task_data[update.task_id.value]

#             self.message_sent += 1
#             driver.sendFrameworkMessage(
#                 executor_id,
#                 slave_id,
#                 'data with a \0 byte'
#             )

#         if (update.state == mesos_pb2.TASK_LOST or
#                 update.state == mesos_pb2.TASK_KILLED or
#                 update.state == mesos_pb2.TASK_FAILED):
#             print "Aborting because task %s is in unexpected state %s with message '%s'" \
#                 % (update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message)
#             driver.abort()

#     def frameworkMessage(self, driver, executorId, slaveId, message):
#         self.message_received += 1

#         # The message bounced back as expected.
#         if message != "data with a \0 byte":
#             print "The returned message data did not match!"
#             print "  Expected: 'data with a \\x00 byte'"
#             print "  Actual:  ", repr(str(message))
#             sys.exit(1)

#         print "Received message:", repr(str(message))

#         if self.message_received == TOTAL_TASKS:
#             if self.message_received != self.message_sent:
#                 print "Sent", self.message_sent,
#                 print "but received", self.message_received
#                 sys.exit(1)
#             print "All tasks done, and all messages received, exiting"
#             driver.stop()


# def main():
#     if len(sys.argv) != 2:
#         print "Usage: %s master" % sys.argv[0]
#         sys.exit(1)

#     executor = mesos_pb2.ExecutorInfo()
#     executor.executor_id.value = "default"
#     executor.command.value = os.path.abspath("./executor.py")
#     executor.name = "Test Executor (Python)"
#     executor.source = "python_test"
#     executor.data = marshal.dumps(dict(os.environ))

#     framework = mesos_pb2.FrameworkInfo()
#     framework.user = "test_user"
#     framework.name = "Test Framework (Python)"
#     framework.principal = "test-framework-python"

#     driver = mesos.MesosSchedulerDriver(
#         SimpleScheduler(executor),
#         framework,
#         sys.argv[1]
#     )

#     status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
#     # Ensure that the driver process terminates.
#     driver.stop()
#     sys.exit(status)


# if __name__ == '__main__':
#     main()
