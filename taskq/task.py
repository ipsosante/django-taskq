import inspect
import json
import logging

from django.utils import timezone

from .json import JSONEncoder
from .models import Task as TaskModel
from .utils import delay_timedelta

logger = logging.getLogger('taskq')


class TaskifyRunContext:
    def __init__(self, task):
        self.task = task


class Taskify:
    def __init__(self, function, name=None):
        self._function = function
        self._name = name

    # If you rename this method, update the code in utils.format_exception_traceback
    def _protected_call(self, kwargs):
        task_args = inspect.getargspec(self._function).args

        if len(task_args) > 0 and task_args[0] == 'self':
            kwargs['self'] = TaskifyRunContext(None)

        self._function(**kwargs)

    def apply(self, *args, **kwargs):

        task_args = inspect.getargspec(self._function).args

        if len(task_args) > 0 and task_args[0] == 'self':
            kwargs['self'] = TaskifyRunContext(None)
            task_args.pop(0)

        for i, arg in enumerate(args):
            kwargs[task_args[i]] = arg

        self._function(**kwargs)

    def apply_async(self, due_at=None, max_retries=3, retry_delay=0,
                    retry_backoff=False, retry_backoff_factor=2, args=[],
                    kwargs={}):

        task_args = inspect.getargspec(self._function).args

        if len(task_args) > 0 and task_args[0] == 'self':
            task_args.pop(0)

        for i, arg in enumerate(args):
            kwargs[task_args[i]] = arg

        func_name = '%s.%s' % (self._function.__module__, self._function.__name__)
        task_name = self._name if self._name else func_name

        if not due_at:
            due_at = timezone.now()

        task = TaskModel()
        task.due_at = due_at
        task.name = task_name
        task.status = TaskModel.STATUS_QUEUED
        task.function_name = func_name
        task.function_args = json.dumps(kwargs, cls=JSONEncoder)
        task.max_retries = max_retries
        task.retry_delay = delay_timedelta(retry_delay)
        task.retry_backoff = retry_backoff
        task.retry_backoff_factor = retry_backoff_factor
        task.save()

        return task


def taskify(func=None, name=None):
    def wrapper_taskify(_func):
        return Taskify(_func, name=name)

    if func is None:
        return wrapper_taskify
    else:
        return wrapper_taskify(func)
