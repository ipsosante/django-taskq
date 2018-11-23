import datetime
import threading
import logging
import time

from time import sleep

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .scheduler import Scheduler
from .models import Task
from .exceptions import Retry, Cancel, TaskLoadingError
from .task import Taskify
from .utils import import_function, delay_timedelta

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

    def stop(self):
        self._should_stop.set()

    @property
    def stopped(self):
        return self._should_stop.is_set()

    def run(self):
        """The main entry point to start the consumer."""
        self.reset_running_tasks()

        scheduler = self.get_scheduler()
        self.run_loop(scheduler)

    def reset_running_tasks(self):
        tasks = Task.objects.filter(status=Task.STATUS_RUNNING)
        tasks.update(status=Task.STATUS_QUEUED)

    def get_scheduler(self):
        if not hasattr(settings, 'TASKQ'):
            schedule = {}
        else:
            schedule = settings.TASKQ.get('schedule', {})

        return Scheduler(schedule)

    def run_loop(self, scheduler):
        while not self.stopped:
            self.create_scheduled_tasks(scheduler)
            self.execute_tasks()

            sleep(self._sleep_rate)

    def create_scheduled_tasks(self, scheduler):
        """Register new tasks for each scheduled (recurring) tasks defined in
        the project settings.
        """
        for scheduled_task_name, scheduled_task in scheduler.tasks.items():
            if not scheduled_task.is_due:
                continue

            due_at = scheduled_task.due_at
            scheduled_task.update_due_at()

            task_exists = Task.objects.filter(
                name=scheduled_task_name,
                due_at=due_at
            ).exists()

            if task_exists:
                continue

            task = Task()
            task.name = scheduled_task_name
            task.due_at = due_at
            task.status = Task.STATUS_QUEUED
            task.function_name = scheduled_task.task
            task.encode_function_args(scheduled_task.args)
            task.max_retries = scheduled_task.max_retries
            task.retry_delay = scheduled_task.retry_delay
            task.retry_backoff = scheduled_task.retry_backoff
            task.retry_backoff_factor = scheduled_task.retry_backoff_factor
            task.save()

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
        function = import_function(task.function_name)

        if not isinstance(function, Taskify):
            msg = f'Function "{task.function_name}" is not a task'
            raise TaskLoadingError(msg)

        args = task.decode_function_args()

        return (function, args)

    def execute_task(self, function, args):
        """Execute the code of the task"""
        with transaction.atomic():
            function.__protected_call__(args)
