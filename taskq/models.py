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
    STATUS_FETCHED = 5  # Task was fetched

    STATUS_CHOICES = (
        (STATUS_QUEUED, "Queued"),
        (STATUS_RUNNING, "Running"),
        (STATUS_SUCCESS, "Success"),
        (STATUS_FAILED, "Failed"),
        (STATUS_CANCELED, "Canceled"),
        (STATUS_FETCHED, "Fetched"),
    )

    uuid = models.CharField(
        max_length=36, unique=True, editable=False, default=generate_task_uuid
    )
    name = models.CharField(
        max_length=255, null=False, blank=True, default="", db_index=True
    )
    function_name = models.CharField(
        max_length=255, null=False, blank=False, default=None
    )
    function_args = models.TextField(null=False, blank=True, default="")
    due_at = models.DateTimeField(null=False, db_index=True)
    status = models.IntegerField(
        choices=STATUS_CHOICES, default=STATUS_QUEUED, db_index=True
    )
    retries = models.IntegerField(null=False, default=0)
    max_retries = models.IntegerField(null=False, default=3)
    retry_delay = models.DurationField(
        null=False, default=datetime.timedelta(seconds=0)
    )
    retry_backoff = models.BooleanField(null=False, default=False)
    retry_backoff_factor = models.IntegerField(null=False, default=2)
    timeout = models.DurationField(null=True, default=None)

    def save(self, *args, **kwargs):
        """Do not allow the Task to be saved with an empty function name."""
        if self.function_name == "":
            raise ValidationError("Task.function_name cannot be empty")

        super().save(*args, **kwargs)

    def encode_function_args(self, args=None, kwargs=None):
        if not kwargs:
            kwargs = {}
        if args:
            kwargs["__positional_args__"] = args
        self.function_args = json.dumps(kwargs, cls=JSONEncoder)

    def decode_function_args(self):
        kwargs = json.loads(self.function_args, cls=JSONDecoder)
        args = kwargs.pop("__positional_args__", [])

        return (args, kwargs)

    def update_due_at_after_failure(self):
        """Update its due_at date taking into account the number of retries and
        its retry_delay, retry_backoff, and retry_backoff_factor properties.
        """
        assert self.retries > 0

        delay = self.retry_delay
        if self.retry_backoff:
            delay_seconds = delay.total_seconds() * (
                self.retry_backoff_factor ** (self.retries - 1)
            )
            delay = datetime.timedelta(seconds=delay_seconds)

        self.due_at = timezone.now() + delay

    def __str__(self):
        status = dict(self.STATUS_CHOICES)[self.status]

        str_repr = f"<{self.__class__.__name__} "
        if self.name:
            str_repr += f"{self.name}, "
        str_repr += f"{self.uuid}, status={status}>"

        return str_repr
