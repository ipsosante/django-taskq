import datetime

from .models import Task


def delay_timedelta(delay):
    """A convenience function to create a timedelta from seconds.

    You can also pass a timedelta directly to this function and it will be
    returned unchanged."""
    if isinstance(delay, datetime.timedelta):
        return delay

    if isinstance(delay, int):
        return datetime.timedelta(seconds=delay)

    raise ValueError('Unexpected delay type')


def task_from_scheduled_task(scheduled_task):
    """Create a new Task initialized with the content of `scheduled_task`.

    Note that the returned Task is not saved in database, you still need to
    call .save() on it.
    """
    task = Task()
    task.name = scheduled_task.name
    task.due_at = scheduled_task.due_at
    task.function_name = scheduled_task.function_name
    task.encode_function_args(scheduled_task.args)
    task.max_retries = scheduled_task.max_retries
    task.retry_delay = scheduled_task.retry_delay
    task.retry_backoff = scheduled_task.retry_backoff
    task.retry_backoff_factor = scheduled_task.retry_backoff_factor

    return task
