import importlib

from django.conf import settings

from taskq.models import Taskify


def taskify(func=None, *, name=None, base=None, **kwargs):
    if base is None:
        default_cls_str = getattr(settings, "TASKQ", {}).get("default_taskify_class")
        if default_cls_str:
            module_name, unit_name = default_cls_str.rsplit(".", 1)
            base = getattr(importlib.import_module(module_name), unit_name)
        else:
            base = Taskify

    def wrapper_taskify(_func):
        return base(_func, name=name, **kwargs)

    if func is None:
        return wrapper_taskify
    else:
        return wrapper_taskify(func)
