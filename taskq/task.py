import uuid
import inspect
import logging
import sys
import json

from taskq.json import JSONEncoder
from taskq.models import Task as TaskModel
from taskq.exceptions import Retry, Cancel

from django.utils import timezone

logger = logging.getLogger('taskq')


class TaskifyRunContext(object):

    def __init__(self, task):
        self.task = task


class Taskify(object):

    _name = None
    _function = None

    def __init__(self, function, name=None):

        self._function = function
        self._name = name

    def __protected_call__(self, task, kwargs):

        try:

            task_args = inspect.getargspec(self._function).args

            if len(task_args) > 0 and task_args[0] == 'self':
                kwargs['self'] = TaskifyRunContext(None)

            self._function(**kwargs)

        except Exception as e:

            if isinstance(e, Retry) or isinstance(e, Cancel):
                raise

            logger.exception('Taskq: ' + str(e), exc_info=sys.exc_info())
            raise Retry()

    def apply(self, *args, **kwargs):

        task_args = inspect.getargspec(self._function).args

        if len(task_args) > 0 and task_args[0] == 'self':
            kwargs['self'] = TaskifyRunContext(None)
            task_args.pop(0)

        for i, arg in enumerate(args):
            kwargs[task_args[i]] = arg

        self._function(**kwargs)

    def apply_async(self, due_at=None, args=[], kwargs={}):

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
        task.uuid = uuid.uuid4()
        task.due_at = due_at
        task.name = task_name
        task.status = TaskModel.STATUS_QUEUED
        task.function_name = func_name
        task.function_args = json.dumps(kwargs, cls=JSONEncoder)
        task.max_retries = 3
        task.save()

        return task


def taskify(*args, **kwargs):

    def _taskify(func):
        return Taskify(func, **kwargs)

    return _taskify
