
class TaskControlException(Exception):

    def __init__(self, **kargs):
        self.__dict__ = kargs

    def __repr__(self):
        return repr(self.__dict__)

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]

    def __contains__(self, item):
        return item in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def has_key(self, k):
        return k in self.__dict__

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()


class Retry(TaskControlException):
    pass


class Cancel(TaskControlException):
    pass


class TaskFatalError(Exception):
    pass

class TaskLoadingError(TaskFatalError):
    """The task's python code failed to load"""
    def __init__(self, exception):
        super().__init__(str(exception))
