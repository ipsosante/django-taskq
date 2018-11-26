from datetime import timedelta

from django.test import TestCase, override_settings
from django.utils.timezone import now

from taskq.consumer import Consumer
from taskq.models import Task
from taskq.exceptions import TaskLoadingError

from .utils import create_task


class ConsumerTestCase(TestCase):

    def test_consumer_can_stop(self):
        """Consumer can be stopped."""
        consumer = Consumer(sleep_rate=0.1)
        consumer.start()
        self.assertTrue(consumer.is_alive())
        consumer.stop()
        consumer.join(timeout=5)
        self.assertFalse(consumer.is_alive())

    def test_consumer_resets_running_tasks_when_started(self):
        """Consumer will reset and run tasks in "RUNNING" state when started.
        """
        task = create_task(status=Task.STATUS_RUNNING)

        consumer = Consumer(sleep_rate=0.1)
        consumer.stop()  # Exit the run loop immediately
        consumer.run()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_QUEUED)

    def test_consumer_run_due_task(self):
        """Consumer will run task at or after their due date."""
        due_at = now() - timedelta(milliseconds=100)
        task = create_task(due_at=due_at)

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_SUCCESS)

    def test_consumer_run_self_cancelling_task(self):
        """Consumer will run a self-cancelling task only once."""
        due_at = now() - timedelta(milliseconds=100)
        task = create_task(
            function_name='tests.fixtures.self_cancelling',
            due_at=due_at
        )

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_CANCELED)

    def test_consumer_wont_load_not_taskified_function(self):
        """Consumer will refuse to load a regular function not decorated with
        @taskify.
        """
        task = create_task(
            function_name='tests.fixtures.naked_function',
        )

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        # FIXME: Right now taskq is raising an exception which is not catched
        # anywhere and will crash the deamon.
        self.assertEqual(task.status, Task.STATUS_FAILED)

    @override_settings(TASKQ={
        'schedule': {
            'my-scheduled-task': {
                'task': 'tests.fixtures.do_nothing',
                'cron': '0 1 * * *',  # crontab(minute=0, hour=1)
            }
        }
    })
    def test_consumer_create_task_for_due_scheduled_task(self):
        """Consumer creates tasks for each scheduled task defined in settings.
        """
        consumer = Consumer()

        # Hack the due_at date to simulate the fact that the task was run once
        # already
        assert len(consumer._scheduler._tasks) == 1
        scheduled_task = consumer._scheduler._tasks[0]
        scheduled_task.due_at -= timedelta(days=1)

        consumer.create_scheduled_tasks()

        queued_tasks = Task.objects.filter(status=Task.STATUS_QUEUED).count()
        self.assertEqual(queued_tasks, 1)


class ImportTaskifiedFunctionTestCase(TestCase):

    def test_can_import_existing_task(self):
        """Consumer can import a valid and existing @taskified function."""
        consumer = Consumer()
        func = consumer.import_taskified_function('tests.fixtures.do_nothing')
        self.assertIsNotNone(func)

    def test_fails_import_non_taskified_functions(self):
        """Consumer raises when trying to import a function not decorated with
        @taskify.
        """
        consumer = Consumer()
        self.assertRaises(TaskLoadingError, consumer.import_taskified_function, 'tests.fixtures.naked_function')

    def test_fails_import_non_existing_module(self):
        """Consumer raises when trying to import a function from a non-existing
        module.
        """
        consumer = Consumer()
        self.assertRaises(TaskLoadingError, consumer.import_taskified_function, 'tests.foobar.nope')

    def test_fails_import_non_existing_function(self):
        """Consumer raises when trying to import a non-existing function."""
        consumer = Consumer()
        self.assertRaises(TaskLoadingError, consumer.import_taskified_function, 'tests.fixtures.not_a_known_function')

    def test_fails_import_function_syntax_error(self):
        """Consumer raises when trying to import a function with a Python
        syntax error.
        """
        consumer = Consumer()
        self.assertRaises(TaskLoadingError, consumer.import_taskified_function,
                          'tests.fixtures_broken.broken_function')
