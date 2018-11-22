from taskq.task import taskify


@taskify()
def do_nothing():
    pass
