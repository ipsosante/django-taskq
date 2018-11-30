import threading
from datetime import timedelta

from django.test import TransactionTestCase, override_settings
from django.utils.timezone import now

from taskq.consumer import Consumer
from taskq.models import Task
from taskq.exceptions import TaskLoadingError

from .utils import create_task


class ConsumerTestCase(TransactionTestCase):

    def test_consumer_can_stop(self):
        """Consumer can be stopped."""
        consumer = Consumer(sleep_rate=0.1)

        thread = threading.Thread(target=consumer.run)
        thread.start()
        self.assertTrue(thread.is_alive())

        consumer.stop()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())

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
        self.assertEqual(task.status, Task.STATUS_FAILED)

    def test_consumer_will_catch_task_exceptions(self):
        """Consumer will catch and log exceptions raised by the tasks."""
        task = create_task(
            function_name='tests.fixtures.failing',
            max_retries=0
        )

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_FAILED)

    def test_consumer_will_retry_after_task_error(self):
        """Consumer will catch the tasks error and retry to run the task
        later.
        """
        task = create_task(
            function_name='tests.fixtures.failing',
            max_retries=3
        )

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_QUEUED)
        self.assertEqual(task.retries, 1)

    def test_consumer_will_retry_at_most_max_retries_times(self):
        """Consumer will not retry the task more than task.max_retries times."""
        task = create_task(
            function_name='tests.fixtures.failing',
            max_retries=2
        )

        consumer = Consumer()

        consumer.execute_tasks()
        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_QUEUED)
        self.assertEqual(task.retries, 1)

        consumer.execute_tasks()
        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_QUEUED)
        self.assertEqual(task.retries, 2)

        consumer.execute_tasks()
        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_FAILED)
        self.assertEqual(task.retries, 2)

    def test_consumer_logs_cleaned_backtrace(self):
        """Consumer will log catched exceptions with internal frames removed
        from the backtrace."""
        create_task(
            function_name='tests.fixtures.failing_alphabet',
            max_retries=0
        )

        consumer = Consumer()

        with self.assertLogs('taskq', level='ERROR') as context_manager:
            consumer.execute_tasks()

        output = ''.join(context_manager.output)
        lines = output.splitlines()

        # First line is our custom message
        self.assertIn('ValueError', lines[0])
        self.assertIn('I don\'t know what comes after "d"', lines[0])

        # Skip the first line (our message) and the second one ("Traceback
        # (most recent call last)")
        lines = lines[2:]

        # Only check the even lines (containing the filename, line and function name)
        relevant_lines = [l for i, l in enumerate(lines) if i % 2 == 0]

        # Check that we are getting the expected function names in the traceback
        expected_functions = ['_protected_call', 'failing_alphabet', 'a', 'b', 'c', 'd']
        for i, expected_function in enumerate(expected_functions):
            self.assertIn(expected_function, relevant_lines[i])

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


class ImportTaskifiedFunctionTestCase(TransactionTestCase):

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
