import threading

from taskq.task import taskify
from taskq.exceptions import Cancel


def naked_function():
    pass


# Keep the @taskify decorator **without** parenthesis here
@taskify
def do_nothing():
    pass


# Keep the @taskify() decorator **with** parenthesis here
@taskify()
def do_nothing_with_parenthesis():
    pass


@taskify
def self_cancelling():
    raise Cancel()


@taskify
def failing():
    raise ValueError("Task is failing")


@taskify
def never_return():
    while True:
        pass


@taskify
def task_return_42():
    return 42


@taskify
def task_add(a, b):
    return a + b


@taskify
def task_divide(a, b=1):
    return a / b


@taskify
def task_update_context(a, context):
    class TestObject:
        pass

    context["obj"] = TestObject()
    return a


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
    global _COUNTER
    global _COUNTER_LOCK
    with _COUNTER_LOCK:
        _COUNTER = 0


def counter_get_value():
    global _COUNTER
    global _COUNTER_LOCK
    with _COUNTER_LOCK:
        return _COUNTER


@taskify()
def counter_increment():
    global _COUNTER
    global _COUNTER_LOCK
    with _COUNTER_LOCK:
        _COUNTER += 1
