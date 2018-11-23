from django.utils.timezone import now

from taskq.models import Task


def create_task(**kwargs):
    task = Task()
    default_function_name = 'tests.fixtures.do_nothing'
    task.function_name = kwargs.get('function_name', default_function_name)
    task.function_args = kwargs.get('function_args', '{}')
    task.due_at = kwargs.get('due_at', now())

    if 'name' in kwargs:
        task.name = kwargs['name']
    if 'status' in kwargs:
        task.status = kwargs['status']
    if 'retries' in kwargs:
        task.retries = kwargs['retries']
    if 'max_retries' in kwargs:
        task.max_retries = kwargs['max_retries']
    if 'retry_delay' in kwargs:
        task.retry_delay = kwargs['retry_delay']
    if 'retry_backoff' in kwargs:
        task.retry_backoff = kwargs['retry_backoff']
    if 'retry_backoff_factor' in kwargs:
        task.retry_backoff_factor = kwargs['retry_backoff_factor']

    task.save()

    return task
