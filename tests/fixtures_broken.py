"""The syntax errors in this modules are added intentionally to test the import
behavior is such cases. Do not fix.
"""

from taskq.task import taskify


@taskify()
def broken_function(a, b):
    return a + 2 b  # noqa: E999
