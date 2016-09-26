import datetime

from django.utils import timezone
from croniter import croniter


class ScheduledTask(object):

    def __init__(self, task=None, cron=None, args=None):

        self.task = task
        self.args = args
        self.cron = cron

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

        for key, value in config.items():
            self.tasks[key] = ScheduledTask(value['task'], value['cron'], value['args'])
