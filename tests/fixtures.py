import threading

from taskq.task import taskify
from taskq.exceptions import Cancel


def naked_function():
    pass


@taskify
def do_nothing():
    pass


@taskify
def self_cancelling():
    raise Cancel()


@taskify
def failing():
    raise ValueError('Task is failing')


@taskify
def failing_alphabet():
    a()


def a():
    b()


def b():
    c()


def c():
    d()


def d():
    raise ValueError('I don\'t know what comes after "d"')


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
