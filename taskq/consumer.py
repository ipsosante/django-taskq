import datetime
import importlib
import logging
import threading

from time import sleep

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .database import LockedTransaction
from .exceptions import Retry, Cancel, TaskLoadingError
from .models import Task
from .scheduler import Scheduler
from .task import Taskify
from .utils import delay_timedelta, task_from_scheduled_task

logger = logging.getLogger('taskq')


class Consumer:
    """Collect and executes tasks when they are due."""

    DEFAULT_SLEEP_RATE = 10  # In seconds

    def __init__(self, sleep_rate=DEFAULT_SLEEP_RATE, execute_tasks_barrier=None):
        """Create a new Consumer.

        :param sleep_rate: The time in seconds the consumer will wait between
        each run loop iteration (mostly usefull when testing).
        :param execute_tasks_barrier: Install the passed barrier in the
        `execute_tasks_barrier` method to test its thread-safety. DO NOT USE
        IN PRODUCTION.
        """
        super().__init__()
        self._should_stop = threading.Event()
        self._scheduler = Scheduler()

        # Test parameters
        self._sleep_rate = sleep_rate
        self._execute_tasks_barrier = execute_tasks_barrier

    def stop(self):
        logger.info('Consumer was asked to quit. '
                    'Terminating process in %ss.', self._sleep_rate)
        self._should_stop.set()

    @property
    def stopped(self):
        return self._should_stop.is_set()

    def run(self):
        """The main entry point to start the consumer run loop."""
        while not self.stopped:
            self.create_scheduled_tasks()
            self.execute_tasks()

            sleep(self._sleep_rate)

    def create_scheduled_tasks(self):
        """Register new tasks for each scheduled (recurring) tasks defined in
        the project settings.
        """
        # Multiple instances of taskq rely on the SHARE ROW EXCLUSIV lock.
        # This mode protects a table against concurrent data changes, and is
        # self-exclusive so that only one session can hold it at a time.
        # See https://www.postgresql.org/docs/10/explicit-locking.html
        with LockedTransaction(Task, "SHARE ROW EXCLUSIVE"):
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

    @transaction.atomic
    def execute_tasks(self):
        due_tasks = self.fetch_due_tasks()

        # Only used when testing. Ask the consumers to wait for each others at
        # the barrier.
        if self._execute_tasks_barrier is not None:
            self._execute_tasks_barrier.wait()

        self.process_tasks(due_tasks)

    def fetch_due_tasks(self):
        # Multiple instances of taskq rely on select_for_update().
        # This mechanism will lock selected rows until the end of the transaction.
        # We also fetch STATUS_RUNNING in case of previous inconsistent state.
        due_tasks = Task.objects.filter(
            Q(status=Task.STATUS_QUEUED) | Q(status=Task.STATUS_RUNNING),
            due_at__lte=timezone.now()
        ).select_for_update(skip_locked=True)

        return due_tasks

    def process_tasks(self, due_tasks):
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
