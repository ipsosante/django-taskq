#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='django-taskq',
    version='0.2.3',
    description='Yet another task queue',
    author='ipso sante',
    author_email='contact@ipsosante.fr',
    url='https://github.com/ipsosante/django-taskq',
    long_description='',
    packages=find_packages(exclude=['tests']),
    zip_safe=False,
    install_requires=[
        'django >= 1.9, < 2.0',
        'croniter >= 0.3, <=0.4',
    ],
)
