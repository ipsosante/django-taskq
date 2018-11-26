# -*- coding: utf-8

import django

DEBUG = True
USE_TZ = True

SECRET_KEY = "%=v}W7z^w8uFjDMnAEZx6/K6dZ/cZTU8kPLZ6E4dskr*wa,LwW(PtFBc4Um7.nw7"

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'taskq',
        'USER': 'postgres',
        'PASSWORD': 'IN0vRycvrF',
        'HOST': 'localhost',
        'PORT': '5433',
    }
}

INSTALLED_APPS = [
    "taskq",
]

if django.VERSION >= (1, 10):
    MIDDLEWARE = ()
else:
    MIDDLEWARE_CLASSES = ()
