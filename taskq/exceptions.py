
class TaskControlException(Exception):
    pass


class Cancel(TaskControlException):
    pass


class TaskFatalError(Exception):
    pass


class TaskLoadingError(TaskFatalError):
    """The task's python code failed to load"""
    def __init__(self, exception):
        super().__init__(str(exception))
