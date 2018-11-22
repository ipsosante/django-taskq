import datetime
import uuid

from django.db import models
from django.core.exceptions import ValidationError


def generate_task_uuid():
    return uuid.uuid4()


class Task(models.Model):
    STATUS_QUEUED = 0  # Task was received and waiting to be run
    STATUS_RUNNING = 1  # Task was started by a worker
    STATUS_SUCCESS = 2  # Task succeeded
    STATUS_FAILED = 3  # Task has failed
    STATUS_CANCELED = 4  # Task was revoked.

    STATUS_CHOICES = (
        (STATUS_QUEUED, 'Queued'),
        (STATUS_RUNNING, 'Running'),
        (STATUS_SUCCESS, 'Success'),
        (STATUS_FAILED, 'Failed'),
        (STATUS_CANCELED, 'Canceled'),
    )

    uuid = models.CharField(max_length=36, unique=True, editable=False, default=generate_task_uuid)
    name = models.CharField(max_length=255, null=False, blank=True, default="")
    function_name = models.CharField(max_length=255, null=False, blank=False, default=None)
    function_args = models.TextField(null=False, blank=True, default="")
    due_at = models.DateTimeField(null=False)
    status = models.IntegerField(choices=STATUS_CHOICES, default=STATUS_QUEUED)
    retries = models.IntegerField(null=False, default=0)
    max_retries = models.IntegerField(null=False, default=3)
    retry_delay = models.DurationField(null=False, default=datetime.timedelta(seconds=0))
    retry_backoff = models.BooleanField(null=False, default=False)
    retry_backoff_factor = models.IntegerField(null=False, default=2)

    def save(self, *args, **kwargs):
        """Do not allow the Task to be saved with an empty function name"""
        if self.function_name == "":
            raise ValidationError('Task.function_name cannot be empty')
        return super().save(*args, **kwargs)

    def __str__(self):
        return '<Task: {name} : {uuid}>'.format(name=self.name, uuid=self.uuid)
