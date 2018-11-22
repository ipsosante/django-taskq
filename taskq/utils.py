import datetime
import importlib


def delay_timedelta(delay):
    """A convenience function to create a timedelta from seconds.

    You can also pass a timedelta directly to this function and it will be
    returned unchanged."""
    if isinstance(delay, datetime.timedelta):
        return delay

    if isinstance(delay, int):
        return datetime.timedelta(seconds=delay)

    raise ValueError('Unexpected delay type')
