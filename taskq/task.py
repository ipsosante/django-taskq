import json
import logging

from django.utils import timezone

from .json import JSONEncoder
from .models import Task as TaskModel
from .utils import delay_timedelta

logger = logging.getLogger('taskq')


class Taskify:
    def __init__(self, function, name=None):
        self._function = function
        self._name = name

    # If you rename this method, update the code in utils.format_exception_traceback
    def _protected_call(self, kwargs):
        self._function(**kwargs)

    def apply(self, *args, **kwargs):
        return self._function(*args, **kwargs)

    def apply_async(self, due_at=None, max_retries=3, retry_delay=0,
                    retry_backoff=False, retry_backoff_factor=2, args=[],
                    kwargs={}):

        if not due_at:
            due_at = timezone.now()

        task = TaskModel()
        task.due_at = due_at
        task.name = self.name
        task.status = TaskModel.STATUS_QUEUED
        task.function_name = self.func_name
        task.function_args = json.dumps(kwargs, cls=JSONEncoder)
        task.max_retries = max_retries
        task.retry_delay = delay_timedelta(retry_delay)
        task.retry_backoff = retry_backoff
        task.retry_backoff_factor = retry_backoff_factor
        task.save()

        return task

    @property
    def func_name(self):
        return '%s.%s' % (self._function.__module__, self._function.__name__)

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
