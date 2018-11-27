import datetime
import importlib
import logging
import threading
import time

from time import sleep

from django.db import transaction
from django.utils import timezone

from .exceptions import Retry, Cancel, TaskLoadingError
from .models import Task
from .scheduler import Scheduler
from .task import Taskify
from .utils import delay_timedelta, task_from_scheduled_task

logger = logging.getLogger('taskq')


class Consumer(threading.Thread):
    """Executes tasks when they are due."""

    DEFAULT_SLEEP_RATE = 10  # In seconds

    # You probably don't want to change default default sleep rate, it's only
    # useful when testing.
    def __init__(self, sleep_rate=DEFAULT_SLEEP_RATE):
        super().__init__()
        self.daemon = True
        timestamp = int(time.time())
        self.name = f"TaskqConsumer-{timestamp}"

        self._sleep_rate = sleep_rate
        self._should_stop = threading.Event()
        self._scheduler = Scheduler()

    def stop(self):
        self._should_stop.set()

    @property
    def stopped(self):
        return self._should_stop.is_set()

    def run(self):
        """The main entry point to start the consumer."""
        self.reset_running_tasks()
        self.run_loop()

    def reset_running_tasks(self):
        tasks = Task.objects.filter(status=Task.STATUS_RUNNING)
        tasks.update(status=Task.STATUS_QUEUED)

    def run_loop(self):
        while not self.stopped:
            self.create_scheduled_tasks()
            self.execute_tasks()

            sleep(self._sleep_rate)

    def create_scheduled_tasks(self):
        """Register new tasks for each scheduled (recurring) tasks defined in
        the project settings.
        """
        for scheduled_task in self._scheduler.due_tasks:
            task_exists = Task.objects.filter(
                function_name=scheduled_task.function_name,
                due_at=scheduled_task.due_at
            ).exists()

            if task_exists:
                continue

            task = task_from_scheduled_task(scheduled_task)
            task.save()

        self._scheduler.update_all_tasks_due_dates()

    def execute_tasks(self):
        due_tasks = Task.objects.filter(
            status=Task.STATUS_QUEUED,
            due_at__lte=timezone.now()
        )

        for due_task in due_tasks:
            self.process_task(due_task)

    def process_task(self, task):
        """Load and execute the task"""
        logger.info('%s : Started', task)

        task.status = Task.STATUS_RUNNING
        task.save()

        try:
            function, args = self.load_task(task)
            self.execute_task(function, args)
            task.status = Task.STATUS_SUCCESS
        except Cancel:
            logger.info('%s : Canceled', task)
            task.status = Task.STATUS_CANCELED
        except Retry as retry:
            logger.info('%s : Failed (will retry)', task)

            task.retries += 1

            if 'max_retries' in retry:
                task.max_retries = retry.max_retries

            if 'retry_delay' in retry:
                task.retry_delay = delay_timedelta(retry.retry_delay)

            if 'retry_backoff' in retry:
                task.retry_backoff = retry.retry_backoff

            if 'retry_backoff_factor' in retry:
                task.retry_backoff_factor = retry.retry_backoff_factor

            if 'args' in retry or 'kwargs' in retry:
                raise NotImplementedError()

            if task.retries >= task.max_retries:
                logger.info('%s : Failed (max retries exceeded)', task)
                task.status = Task.STATUS_FAILED
            else:
                delay = task.retry_delay
                if task.retry_backoff:
                    delay_seconds = (
                        delay.total_seconds()
                        * (task.retry_backoff_factor ** (task.retries - 1))
                    )
                    delay = datetime.timedelta(seconds=delay_seconds)
                task.due_at = timezone.now() + delay

                task.status = Task.STATUS_QUEUED
        finally:
            task.save()

    def load_task(self, task):
        function = self.import_taskified_function(task.function_name)
        args = task.decode_function_args()

        return (function, args)

    def import_taskified_function(self, import_path):
        """Load a @taskified function from a python module.

        Returns TaskLoadingError if loading of the function failed.
        """
        # https://stackoverflow.com/questions/3606202
        module_name, unit_name = import_path.rsplit('.', 1)
        try:
            module = importlib.import_module(module_name)
        except (ImportError, SyntaxError) as e:
            raise TaskLoadingError(e)

        try:
            obj = getattr(module, unit_name)
        except AttributeError as e:
            raise TaskLoadingError(e)

        if not isinstance(obj, Taskify):
            msg = f'Object "{import_path}" is not a task'
            raise TaskLoadingError(msg)

        return obj

    def execute_task(self, function, args):
        """Execute the code of the task"""
        with transaction.atomic():
            function._protected_call(args)
