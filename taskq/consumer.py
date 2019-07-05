import importlib
import logging
import threading

from time import sleep

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .database import LockedTransaction
from .exceptions import Cancel, TaskLoadingError, TaskFatalError
from .models import Task
from .scheduler import Scheduler
from .task import Taskify
from .utils import task_from_scheduled_task, traceback_filter_taskq_frames, ordinal

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
                    'Terminating process in less than %ss.', self._sleep_rate)
        self._should_stop.set()

    @property
    def stopped(self):
        return self._should_stop.is_set()

    def run(self):
        """The main entry point to start the consumer run loop."""
        logger.info('Consumer started.')

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
                    name=scheduled_task.name,
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
        if not task.retries:
            logger.info('%s : Started', task)
        else:
            nth = ordinal(task.retries)
            logger.info('%s : Started (%s retry)', task, nth)

        task.status = Task.STATUS_RUNNING
        task.save()

        try:
            function, args, kwargs = self.load_task(task)
            self.execute_task(function, args, kwargs)
        except TaskFatalError as e:
            logger.info('%s : Fatal error', task)
            self.fail_task(task, e)
        except Cancel:
            logger.info('%s : Canceled', task)
            task.status = Task.STATUS_CANCELED
        except Exception as e:
            if task.retries < task.max_retries:
                logger.info('%s : Failed, will retry', task)
                self.retry_task_later(task)
            else:
                logger.info('%s : Failed, exceeded max retries', task)
                self.fail_task(task, e)
        else:
            logger.info('%s : Success', task)
            task.status = Task.STATUS_SUCCESS
        finally:
            task.save()

    def retry_task_later(self, task):
        task.status = Task.STATUS_QUEUED
        task.retries += 1
        task.update_due_at_after_failure()

    def fail_task(self, task, error):
        task.status = Task.STATUS_FAILED
        exc_traceback = traceback_filter_taskq_frames(error)
        type_name = type(error).__name__
        exc_info = (type(error), error, exc_traceback)
        logger.exception('%s : %s %s', task, type_name, error, exc_info=exc_info)

    def load_task(self, task):
        function = self.import_taskified_function(task.function_name)
        args, kwargs = task.decode_function_args()

        return (function, args, kwargs)

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

    def execute_task(self, function, args, kwargs):
        """Execute the code of the task"""
        with transaction.atomic():
            function._protected_call(args, kwargs)
