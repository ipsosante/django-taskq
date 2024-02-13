import copy
import datetime
import importlib
import logging
import uuid

from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone

from .exceptions import TaskLoadingError
from .json import JSONDecoder, JSONEncoder
from .utils import parse_timedelta

logger = logging.getLogger("taskq")


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
    function_args = models.JSONField(
        null=False, default=dict, encoder=JSONEncoder, decoder=JSONDecoder
    )

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

    @staticmethod
    def _function_args_and_kwargs_to_dict(args=None, kwargs=None):
        args_dict = {}
        if kwargs:
            args_dict.update(copy.deepcopy(kwargs))
        if args:
            args_dict["__positional_args__"] = copy.deepcopy(args)
        return args_dict

    @staticmethod
    def _dict_to_function_args_and_kwargs(args_dict):
        args_dict = copy.deepcopy(args_dict)
        args = args_dict.pop("__positional_args__", [])
        kwargs = args_dict
        return args, kwargs

    def encode_function_args(self, args=None, kwargs=None):
        self.function_args = self._function_args_and_kwargs_to_dict(args, kwargs)

    def decode_function_args(self):
        return self._dict_to_function_args_and_kwargs(self.function_args)

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

    def load_task(self):
        taskified_function = self.import_taskified_function(self.function_name)
        args, kwargs = self.decode_function_args()

        return (taskified_function, args, kwargs)

    @staticmethod
    def import_taskified_function(import_path):
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

    def execute(self):
        taskified_function, args, kwargs = self.load_task()
        taskified_function._protected_call(args, kwargs)

    def __str__(self):
        status = dict(self.STATUS_CHOICES)[self.status]

        str_repr = f"<{self.__class__.__name__} "
        if self.name:
            str_repr += f"{self.name}, "
        str_repr += f"{self.uuid}, status={status}>"

        return str_repr


class Taskify:
    def __init__(self, function, name=None):
        self._function = function
        self._name = name

    def __call__(self, *args, **kwargs):
        return self._function(*args, **kwargs)

    # If you rename this method, update the code in utils.traceback_filter_taskq_frames
    def _protected_call(self, args, kwargs):
        self.__call__(*args, **kwargs)

    def apply(self, *args, **kwargs):
        return self.__call__(*args, **kwargs)

    def apply_async(
        self,
        due_at=None,
        max_retries=3,
        retry_delay=0,
        retry_backoff=False,
        retry_backoff_factor=2,
        timeout=None,
        args=None,
        kwargs=None,
    ):
        """Apply a task asynchronously.
        .
                :param Tuple args: The positional arguments to pass on to the task.

                :parm Dict kwargs: The keyword arguments to pass on to the task.

                :parm due_at: When the task should be executed. (None = now).
                :type due_at: timedelta or None

                :param timeout: The maximum time a task may run.
                                (None = no timeout)
                                (int = number of seconds)
                :type timeout: timedelta or int or None
        """

        if due_at is None:
            due_at = timezone.now()
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        task = Task()
        task.due_at = due_at
        task.name = self.name
        task.status = Task.STATUS_QUEUED
        task.function_name = self.func_name
        task.encode_function_args(args, kwargs)
        task.max_retries = max_retries
        task.retry_delay = parse_timedelta(retry_delay)
        task.retry_backoff = retry_backoff
        task.retry_backoff_factor = retry_backoff_factor
        task.timeout = parse_timedelta(timeout, nullable=True)
        task.save()

        return task

    @property
    def func_name(self):
        return "%s.%s" % (self._function.__module__, self._function.__name__)

    @property
    def name(self):
        return self._name if self._name else self.func_name
