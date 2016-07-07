In one terminal, run:

    ./manage.py taskqrunworker

In another terminal:

    ./manage.py shell

Commands to try out:

    from example.tasks import add
    add.apply_async(16, 2)
