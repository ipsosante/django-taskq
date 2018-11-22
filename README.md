# django-taskq

[![MIT](https://img.shields.io/github/license/ipsosante/django-taskq.svg)](https://tldrlegal.com/license/mit-license)

django-taskq is an asynchronous task queue/job queue for Django. It's a lightweight alternative to [Celery](http://www.celeryproject.org/).

## Usage

Install django-taskq:

    pip install django-taskq


Run the main worker with:

    ./manage.py taskqrunworker

Then open a Django shell and add tasks:

    ./manage.py shell
    >>> from example.tasks import add
    >>> add.apply_async(16, 2)

## Contributing

Setup the development environment with

    python -m venv $(pwd)/virtualenv
    . ./virtualenv/bin/activate
	pip install -r requirements_dev.txt

Set your PYTHONPATH to the developement directory:

    export PYTHONPATH=$PYTHONPATH:$PWD

Run the test suite with

    python runtests.py

If you want to run `django-admin` commands, you'll need to set `DJANGO_SETTINGS_MODULE` to a valid settings module, e.g.:

    export DJANGO_SETTINGS_MODULE=tests.settings

## License

django-taskq is released under the MIT license. See LICENSE for details.
