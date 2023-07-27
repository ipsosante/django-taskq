import importlib
import logging
import threading
from time import sleep

import timeout_decorator
from django.conf import settings
from django.db import transaction, DatabaseError
from django.db.models import Q
from django.utils import timezone
from django_pglocks import advisory_lock

from .constants import TASKQ_DEFAULT_CONSUMER_SLEEP_RATE, TASKQ_DEFAULT_TASK_TIMEOUT
from .exceptions import Cancel, TaskLoadingError, TaskFatalError
from .models import Task
from .scheduler import Scheduler
from .task import Taskify
from .utils import task_from_scheduled_task, traceback_filter_taskq_frames, ordinal

logger = logging.getLogger("taskq")


class Consumer:
    """Collect and executes tasks when they are due."""

    def __init__(
        self, sleep_rate=TASKQ_DEFAULT_CONSUMER_SLEEP_RATE, execute_tasks_barrier=None
    ):
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
        self._fetched_tasks_count_above_error_threshold_counter = 0

        # Test parameters
        self._sleep_rate = sleep_rate
        self._execute_tasks_barrier = execute_tasks_barrier

    def stop(self):
        logger.info(
            "Consumer was asked to quit. " "Terminating process in less than %ss.",
            self._sleep_rate,
        )
        self._should_stop.set()

    @property
    def stopped(self):
        return self._should_stop.is_set()

    def run(self):
        """The main entry point to start the consumer run loop."""
        logger.info("Consumer started.")

        while not self.stopped:
            self.create_scheduled_tasks()
            self.execute_tasks()

            sleep(self._sleep_rate)

    def create_scheduled_tasks(self):
        """Register new tasks for each scheduled (recurring) tasks defined in
        the project settings.
        """
        due_tasks = self._scheduler.due_tasks

        if not due_tasks:
            return

        # Multiple instances of taskq rely on an advisory lock.
        # This lock is self-exclusive so that only one session can hold it at a time.
        # https://www.postgresql.org/docs/11/explicit-locking.html#ADVISORY-LOCKS
        with advisory_lock("taskq_create_scheduled_tasks"):
            for scheduled_task in due_tasks:
                task_exists = Task.objects.filter(
                    name=scheduled_task.name, due_at=scheduled_task.due_at
                ).exists()

                if task_exists:
                    continue

                task = task_from_scheduled_task(scheduled_task)
                task.save()

        self._scheduler.update_all_tasks_due_dates()

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

        with transaction.atomic():
            due_tasks_qs = Task.objects.filter(
                Q(status=Task.STATUS_QUEUED), due_at__lte=timezone.now()
            ).select_for_update(skip_locked=True)

            due_tasks = list(due_tasks_qs)
            for task in due_tasks:
                task.status = Task.STATUS_FETCHED

            Task.objects.bulk_update(due_tasks, fields=["status"])

        self._log_fetched_tasks_count(len(due_tasks))

        return due_tasks

    def _log_fetched_tasks_count(self, task_count):
        if task_count:
            logger.info(f"{task_count} tasks fetched")

        log_threshold = getattr(
            settings, "TASKQ_FETCHED_TASKS_COUNT_LOGGED_AS_ERROR_THRESHOLD", None
        )
        if log_threshold and task_count >= log_threshold:
            self._fetched_tasks_count_above_error_threshold_counter += 1
            counter_trigger = getattr(
                settings,
                "TASKQ_FETCHED_TASKS_COUNT_ABOVE_ERROR_THRESHOLD_COUNTER_TRIGGER",
                1,
            )
            if (
                self._fetched_tasks_count_above_error_threshold_counter
                >= counter_trigger
            ):
                logger.error(
                    f"more than {log_threshold} tasks fetched",
                    extra={
                        "task count": task_count,
                        "task count above threshold counter": self._fetched_tasks_count_above_error_threshold_counter,
                    },
                )
        else:
            self._fetched_tasks_count_above_error_threshold_counter = 0

    def process_tasks(self, due_tasks):
        for due_task in due_tasks:
            self.process_task(due_task)

    def process_task(self, task):
        """Load and execute the task"""
        if task.timeout is None:
            timeout = getattr(
                settings, "TASKQ_TASK_TIMEOUT", TASKQ_DEFAULT_TASK_TIMEOUT
            )
        else:
            timeout = task.timeout

        if not task.retries:
            logger.info("%s : Started", task)
        else:
            nth = ordinal(task.retries)
            logger.info("%s : Started (%s retry)", task, nth)

        def _execute_task():
            function, args, kwargs = self.load_task(task)
            self.execute_task(function, args, kwargs)

        try:
            task.status = Task.STATUS_RUNNING
            task.save()

            try:
                if timeout.total_seconds():
                    assert threading.current_thread() is threading.main_thread()
                    timeout_decorator.timeout(
                        seconds=timeout.total_seconds(), use_signals=True
                    )(_execute_task)()
                else:
                    _execute_task()

            except TaskFatalError as e:
                logger.info("%s : Fatal error", task)
                self.fail_task(task, e)
            except Cancel:
                logger.info("%s : Canceled", task)
                task.status = Task.STATUS_CANCELED
            except timeout_decorator.TimeoutError as e:
                logger.info("%s : Timed out", task)
                self.fail_task(task, e)
            except Exception as e:
                if task.retries < task.max_retries:
                    logger.info("%s : Failed, will retry", task)
                    self.retry_task_later(task)
                else:
                    logger.info("%s : Failed, exceeded max retries", task)
                    self.fail_task(task, e)
            else:
                logger.info("%s : Success", task)
                task.status = Task.STATUS_SUCCESS
            finally:
                task.save()
        except DatabaseError:
            logger.error("%s : DB error, couldn't update task status", task)

    def retry_task_later(self, task):
        task.status = Task.STATUS_QUEUED
        task.retries += 1
        task.update_due_at_after_failure()

    def fail_task(self, task, error):
        task.status = Task.STATUS_FAILED
        exc_traceback = traceback_filter_taskq_frames(error)
        type_name = type(error).__name__
        exc_info = (type(error), error, exc_traceback)
        logger.exception("%s : %s %s", task, type_name, error, exc_info=exc_info)

    def load_task(self, task):
        function = self.import_taskified_function(task.function_name)
        args, kwargs = task.decode_function_args()

        return (function, args, kwargs)

    def import_taskified_function(self, import_path):
        """Load a @taskified function from a python module.

        Returns TaskLoadingError if loading of the function failed.
        """
        # https://stackoverflow.com/questions/3606202
        module_name, unit_name = import_path.rsplit(".", 1)
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
