# -*- coding: utf-8

import django

DEBUG = True
USE_TZ = True

SECRET_KEY = "%=v}W7z^w8uFjDMnAEZx6/K6dZ/cZTU8kPLZ6E4dskr*wa,LwW(PtFBc4Um7.nw7"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

INSTALLED_APPS = [
    "taskq",
]

SITE_ID = 1

if django.VERSION >= (1, 10):
    MIDDLEWARE = ()
else:
    MIDDLEWARE_CLASSES = ()
