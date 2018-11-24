from time import sleep

from taskq.task import taskify
from taskq.exceptions import Cancel


def naked_function():
    pass


@taskify()
def do_nothing():
    pass


@taskify()
def do_nothing_sleep():
    print("do_nothing_sleep")
    sleep(0.1)


@taskify()
def self_cancelling():
    raise Cancel()
