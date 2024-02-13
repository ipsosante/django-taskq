#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="django-taskq",
    version="0.6.0",
    description="Yet another task queue",
    author="ipso sante",
    author_email="contact@ipsosante.fr",
    url="https://github.com/ipsosante/django-taskq",
    long_description="",
    packages=find_packages(exclude=["tests"]),
    zip_safe=False,
    install_requires=[
        "django < 5.0.0",
        "croniter >= 1.4.1",
        "django-pglocks >= 1.0.4",
        "timeout-decorator >= 0.5.0",
    ],
)
