import logging

from django.utils import timezone

from .models import Task as TaskModel
from .utils import parse_timedelta

logger = logging.getLogger("taskq")


class Taskify:
    def __init__(self, function, name=None):
        self._function = function
        self._name = name

    # If you rename this method, update the code in utils.traceback_filter_taskq_frames
    def _protected_call(self, args, kwargs):
        self._function(*args, **kwargs)

    def apply(self, *args, **kwargs):
        return self._function(*args, **kwargs)

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

        task = TaskModel()
        task.due_at = due_at
        task.name = self.name
        task.status = TaskModel.STATUS_QUEUED
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


def taskify(func=None, name=None):
    def wrapper_taskify(_func):
        return Taskify(_func, name=name)

    if func is None:
        return wrapper_taskify
    else:
        return wrapper_taskify(func)
