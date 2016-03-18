# -*- coding:utf-8 -*-

ADMINS = (
    ('Antonin Blanc', 'antonin.blanc@ipsosante.fr'),
)

SECRET_KEY = 'foo'

INSTALLED_APPS = [
    'taskq',
    'example',
]

TASKQ = {
    'schedule': {
        'test1': {
            'cron': '*/1 * * * *',
            'task': 'example.tasks.add',
            'args': {'x': 1, 'y': 2},
        },
    }
}

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'mydatabase.sqlite',
    }
}

ALLOWED_HOSTS = '*'

EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = ''
EMAIL_PORT = 465
EMAIL_USE_SSL = True
EMAIL_HOST_USER = ''
EMAIL_HOST_PASSWORD = ''
SERVER_EMAIL = 'django@ipsosante.fr'

ROOT_URLCONF = None

DEBUG = True

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'verbose',
        },
        'mail_admins': {
            'level': 'ERROR',
            'class': 'django.utils.log.AdminEmailHandler',
            'include_html': True,
        },
    },
    'loggers': {
        'taskq': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': True,
        },
        'django.request': {
            'handlers': ['console'],
            'level': 'WARNING',
            'propagate': True,
        },
    },
}
