import datetime
import json
import uuid

from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone

from .json import JSONDecoder, JSONEncoder


def generate_task_uuid():
    return str(uuid.uuid4())


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

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):
        """Do not allow the Task to be saved with an empty function name."""
        if self.function_name == "":
            raise ValidationError('Task.function_name cannot be empty')

        super().save(force_insert=force_insert, force_update=force_update,
                     using=None, update_fields=None)

    def encode_function_args(self, args=None, kwargs=None):
        if not kwargs:
            kwargs = {}
        if args:
            kwargs['__positional_args__'] = args
        self.function_args = json.dumps(kwargs, cls=JSONEncoder)

    def decode_function_args(self):
        kwargs = json.loads(self.function_args, cls=JSONDecoder)
        args = kwargs.pop('__positional_args__', [])

        return (args, kwargs)

    def update_due_at_after_failure(self):
        """Update its due_at date taking into account the number of retries and
        its retry_delay, retry_backoff, and retry_backoff_factor properties.
        """
        assert self.retries > 0

        delay = self.retry_delay
        if self.retry_backoff:
            delay_seconds = delay.total_seconds() * (self.retry_backoff_factor ** (self.retries - 1))
            delay = datetime.timedelta(seconds=delay_seconds)

        self.due_at = timezone.now() + delay

    def __str__(self):
        s = f'{self.name}, ' if self.name else ''
        s += str(self.uuid)
        return s
