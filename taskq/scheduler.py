import datetime

from django.conf import settings
from django.utils import timezone
from croniter import croniter

from .utils import delay_timedelta


class ScheduledTask:

    def __init__(self, name, task, cron, args=None, max_retries=3,
                 retry_delay=0, retry_backoff=False, retry_backoff_factor=2):
        self.name = name
        self.task = task  # The function to be executed
        self.args = args if args else {}
        self.cron = cron
        self.max_retries = max_retries
        self.retry_delay = delay_timedelta(retry_delay)
        self.retry_backoff = retry_backoff
        self.retry_backoff_factor = retry_backoff_factor

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



class Scheduler():
    def __init__(self, config):
        self.tasks = {}

        for task_name, task_config in config.items():
            self.tasks[task_name] = ScheduledTask(**task_config)
