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
