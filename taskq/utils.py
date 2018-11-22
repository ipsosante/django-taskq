import datetime
import importlib

from .exceptions import TaskLoadingError

# https://stackoverflow.com/questions/3606202
def import_function(import_path):
    module_name, unit_name = import_path.rsplit('.', 1)
    try:
        module = importlib.import_module(module_name)
    except (ImportError, SyntaxError) as e:
        raise TaskLoadingError(e)

    try:
        func = getattr(module, unit_name)
    except AttributeError as e:
        raise TaskLoadingError(e)

    return func


def delay_timedelta(delay):
    """A convenience function to create a timedelta from seconds.

    You can also pass a timedelta directly to this function and it will be
    returned unchanged."""
    if isinstance(delay, datetime.timedelta):
        return delay

    if isinstance(delay, int):
        return datetime.timedelta(seconds=delay)

    raise ValueError('Unexpected delay type')
