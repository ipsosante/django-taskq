import datetime

from django.utils import timezone
from croniter import croniter


class ScheduledTask(object):

    def __init__(self, task, cron, args=None, max_retries=3, retry_delay=0, retry_backoff=False, retry_backoff_factor=2):

        self.task = task
        self.args = args if args else {}
        self.cron = cron
        self.max_retries = max_retries
        self.retry_delay = retry_delay if isinstance(retry_delay, datetime.timedelta) else datetime.timedelta(seconds = retry_delay)
        self.retry_backoff = retry_backoff
        self.retry_backoff_factor = retry_backoff_factor

        self.update_due_at()

    def update_due_at(self):

        now = timezone.now()
        cron = croniter(self.cron, now)
        self.due_at = cron.get_next(datetime.datetime)

    def is_due(self):

        now = timezone.now()
        return (self.due_at - now).total_seconds() < 0


class Scheduler(object):

    tasks = {}

    def __init__(self, config):

        for task_name, task_config in config.items():
            self.tasks[task_name] = ScheduledTask(**task_config)
