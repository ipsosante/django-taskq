import datetime

from croniter import croniter
from django.conf import settings
from django.utils import timezone

from .models import Task
from .utils import parse_timedelta


class ScheduledTask:
    def __init__(
        self,
        name,
        task,
        cron,
        args=None,
        max_retries=3,
        retry_delay=0,
        retry_backoff=False,
        retry_backoff_factor=2,
        timeout=None,
    ):
        self.name = name
        self.task = task  # The function to be executed
        self.args = args if args else {}
        self.cron = cron
        self.max_retries = max_retries
        self.retry_delay = parse_timedelta(retry_delay)
        self.retry_backoff = retry_backoff
        self.retry_backoff_factor = retry_backoff_factor
        self.timeout = parse_timedelta(timeout, nullable=True)

        self.update_due_at()

    def update_due_at(self):
        """Update self.due_at to the next datetime at which this task must be
        ran"""
        now = timezone.now()
        cron = croniter(self.cron, now)
        self.due_at = cron.get_next(datetime.datetime)

    @property
    def function_name(self):
        """Convenience alias for self.task"""
        return self.task

    @property
    def is_due(self):
        now = timezone.now()
        return self.due_at <= now

    @property
    def as_task(self):
        """
        Note that the returned Task is not saved in database, you still need to call.save() on it.
        """
        task = Task()
        task.name = self.name
        task.due_at = self.due_at
        task.function_name = self.function_name
        task.encode_function_args(kwargs=self.args)
        task.max_retries = self.max_retries
        task.retry_delay = self.retry_delay
        task.retry_backoff = self.retry_backoff
        task.retry_backoff_factor = self.retry_backoff_factor
        task.timeout = self.timeout

        return task

    def create_task(self):
        self.as_task.save()


class Scheduler:
    def __init__(self):
        taskq_config = getattr(settings, "TASKQ", {})
        schedule = taskq_config.get("schedule", {})

        self._tasks = []

        for task_name, task_config in schedule.items():
            new_task = ScheduledTask(name=task_name, **task_config)
            self._tasks.append(new_task)

    @property
    def due_tasks(self):
        """Returns all the task which are due (task.is_due == True)"""
        return [t for t in self._tasks if t.is_due]

    def update_all_tasks_due_dates(self):
        """Update the due_at property of all tasks"""
        for task in self.due_tasks:
            task.update_due_at()
