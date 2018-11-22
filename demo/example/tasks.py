from taskq.task import taskify


@taskify()
def add(self, x, y):
    return x + y
