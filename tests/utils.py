from functools import partial
import threading

from django.db import connections
from django.utils.timezone import now

from taskq.consumer import Consumer
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


def create_background_consumers(count, before_start=None, target='run', *args, **kwargs):
    """Create new Consumer instances on background threads, starts them, and
    return a tuple ([consumer], [thread]).

    :param count: The number of Consumer instances to start.
    :param before_start: A callable that will be called with each consumer as
    an argument before the threads are started.
    :param target: The name of the method of the Consumer that will be run.

    The remaining arguments of the function are passed to the __init__() method
    of each Consumer.
    """

    consumers = []
    threads = []
    for _ in range(count):
        consumer = Consumer(*args, **kwargs)
        consumers.append(consumer)

        thread_target = partial(_consumer_run_and_close_connection, consumer, target)
        thread = threading.Thread(target=thread_target)
        threads.append(thread)

    if before_start:
        for consumer in consumers:
            before_start(consumer)

    for thread in threads:
        thread.start()

    return (consumers, threads)


def _consumer_run_and_close_connection(consumer, target):
    """Execute the `target` method of `consumer`, then manually close all
    database connections.

    These aren't automatically closed by Django.
    See https://code.djangoproject.com/ticket/22420.
    """
    method = getattr(consumer, target)
    method()

    for connection in connections.all():
        connection.close()
