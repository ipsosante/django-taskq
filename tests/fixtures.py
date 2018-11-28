import threading

from taskq.task import taskify
from taskq.exceptions import Cancel


def naked_function():
    pass


@taskify()
def do_nothing():
    pass


@taskify()
def self_cancelling():
    raise Cancel()


###############################################################################

_COUNTER = 0
_COUNTER_LOCK = threading.Lock()


def counter_reset():
    with _COUNTER_LOCK:
        global _COUNTER
        _COUNTER = 0


def counter_get_value():
    with _COUNTER_LOCK:
        global _COUNTER
        return _COUNTER


@taskify()
def counter_increment():
    with _COUNTER_LOCK:
        global _COUNTER
        _COUNTER += 1
