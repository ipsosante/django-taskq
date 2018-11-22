#!/usr/bin/env python

from setuptools import setup

setup(
    name='django-taskq',
    version='0.1.0',
    description='Yet another task queue',
    author='ipso sante',
    author_email='contact@ipsosante.fr',
    url='https://github.com/ipsosante/django-taskq',
    long_description='',
    packages=[
        'taskq',
    ],
    zip_safe=False,
    install_requires=[
        'django >= 1.9, < 2.0',
        'croniter >= 0.3, <=0.4',
    ],
)
