import json
import uuid
import threading
import importlib
import logging
from time import sleep

from django.conf import settings
from django.utils import timezone

from taskq.scheduler import Scheduler
from taskq.models import Task as TaskModel
from taskq.json import JSONDecoder, JSONEncoder
from taskq.exceptions import Retry, Cancel
from taskq.task import Taskify

logger = logging.getLogger('taskq')


class Consumer(threading.Thread):
    """
    Executes tasks when they are due.
    """

    SLEEP_RATE = 10  # In seconds

    def __init__(self):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.stop = threading.Event()

    def stop(self):
        self.stop.set()

    def stopped(self):
        return self.stop.is_set()

    def run(self):

        # Reset

        tasks = TaskModel.objects.filter(status=TaskModel.STATUS_RUNNING)
        for task in tasks:
            task.status = TaskModel.STATUS_QUEUED
            task.save()

        # ...

        if hasattr(settings, 'TASKQ') and settings.TASKQ['schedule']:
            schedule = settings.TASKQ['schedule']
        else:
            schedule = {}

        scheduler = Scheduler(schedule)

        # ...

        while not self.stopped():

            for scheduled_task_name, scheduled_task in scheduler.tasks.items():

                if not scheduled_task.is_due():
                    continue

                due_at = timezone.make_aware(scheduled_task.due_at, timezone.get_current_timezone())
                scheduled_task.update_due_at()

                tmp = TaskModel.objects.filter(name=scheduled_task_name, due_at=due_at)

                if len(tmp):
                    continue

                task = TaskModel()
                task.uuid = uuid.uuid4()
                task.name = scheduled_task_name
                task.due_at = due_at
                task.status = TaskModel.STATUS_QUEUED
                task.function_name = scheduled_task.task
                task.function_args = json.dumps(scheduled_task.args, cls=JSONEncoder)
                task.max_retries = 3
                task.save()

            # ...

            tasks = TaskModel.objects.filter(status=TaskModel.STATUS_QUEUED, due_at__lte=timezone.now())

            for task in tasks:

                self.process_task(task)

            to_sleep = Consumer.SLEEP_RATE
            sleep(to_sleep)

    def process_task(self, task):

        logger.info('Task (%s) : Run', task)

        task.status = TaskModel.STATUS_RUNNING
        task.save()

        def import_function(name):
            module_path, comp = name.rsplit('.', 1)
            module = importlib.import_module(module_path)
            return getattr(module, comp)

        try:
            function = import_function(task.function_name)
        except Exception:
            raise Exception("Unable to find task function.")

        if not isinstance(function, Taskify):
            raise Exception("Function is not a task.")

        args = json.loads(task.function_args, cls=JSONDecoder)

        try:

            from django.db import transaction

            with transaction.atomic():
                function.__protected_call__(task, args)

            task.status = TaskModel.STATUS_SUCCESS
            task.save()

        except Cancel as e:

            logger.info('Task (%s) : Canceled', task)

            task.status = TaskModel.STATUS_REVOKED
            task.save()

        except Retry as e:

            logger.info('Task (%s) : Failed', task)

            task.retries += 1

            if 'max_retries' in e:
                task.max_retries = e.max_retries

            if 'countdown' in e:
                raise NotImplementedError()

            if 'args' in e or 'kwargs' in e:
                raise NotImplementedError()

            if task.retries >= task.max_retries:

                logger.info('Task (%s) : Failed', task)

                task.status = TaskModel.STATUS_FAILED

            else:

                task.status = TaskModel.STATUS_QUEUED

            task.save()
