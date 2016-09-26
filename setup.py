#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='django-taskq',
    version='0.0.3',
    description='yet another task queue',
    author='ipso sante',
    author_email='contact@ipsosante.fr',
    url='https://github.com/ipsosante/django-taskq',
    long_description='',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        'django',
        'croniter',
    ],
)
