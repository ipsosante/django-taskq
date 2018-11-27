# django-taskq

[![MIT](https://img.shields.io/github/license/ipsosante/django-taskq.svg)](https://tldrlegal.com/license/mit-license)
[![Build Status](https://img.shields.io/travis/ipsosante/django-taskq/master.svg)](https://travis-ci.org/ipsosante/django-taskq)
[![Codecov](https://img.shields.io/codecov/c/github/ipsosante/django-taskq/master.svg)](https://codecov.io/gh/ipsosante/django-taskq)

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

Setup the PostgreSQL database:

    brew install postgresql@10
    initdb /path/to/database/storage  # For example /usr/local/var/postgres

    # Start PostgreSQL on port 5433
    pg_ctl -D /path/to/database/storage -p 5433 start

    # Create a postgres user allowed to create our test database
    psql -p 5433 -d postgres -c "CREATE USER postgres WITH PASSWORD 'IN0vRycvrF' CREATEDB"

Set your PYTHONPATH to the developement directory:

    export PYTHONPATH=$PYTHONPATH:$PWD

Run the test suite with

    pytest


----------

To collect coverage data run the test suite with

    pytest --cov=taskq tests/

To collect coverage data as HTML and view it in your browser, use

    pytest --cov-report html --cov=taskq tests/; open htmlcov/index.html

----------

If you want to run `django-admin` commands, you'll need to set `DJANGO_SETTINGS_MODULE` to a valid settings module, e.g.:

    export DJANGO_SETTINGS_MODULE=tests.settings

## License

django-taskq is released under the MIT license. See LICENSE for details.
