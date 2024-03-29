from datetime import timedelta
from logging import ERROR
from unittest.mock import patch

from django.db import OperationalError
from django.db.models import Model
from django.test import TransactionTestCase, override_settings
from django.utils.timezone import now

from taskq.consumer import Consumer
from taskq.models import Task
from .utils import create_task, create_background_consumers


class ConsumerTestCase(TransactionTestCase):
    def test_consumer_can_stop(self):
        """Consumer can be stopped."""
        consumers, threads = create_background_consumers(1, sleep_rate=0.1)
        consumer = consumers[0]
        thread = threads[0]

        self.assertTrue(thread.is_alive())

        consumer.stop()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())

    def test_consumer_run_due_task(self):
        """Consumer will run task at or after their due date."""
        due_at = now() - timedelta(milliseconds=100)
        task = create_task(
            function_name="tests.fixtures.task_add",
            due_at=due_at,
            function_args={"a": 1, "b": 2},
        )

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_SUCCESS)

    def test_consumer_run_self_cancelling_task(self):
        """Consumer will run a self-cancelling task only once."""
        due_at = now() - timedelta(milliseconds=100)
        task = create_task(
            function_name="tests.fixtures.self_cancelling", due_at=due_at
        )

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_CANCELED)

    def test_consumer_wont_load_not_taskified_function(self):
        """Consumer will refuse to load a regular function not decorated with
        @taskify.
        """
        task = create_task(function_name="tests.fixtures.naked_function")

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_FAILED)

    def test_consumer_will_catch_task_exceptions(self):
        """Consumer will catch and log exceptions raised by the tasks."""
        task = create_task(function_name="tests.fixtures.failing", max_retries=0)

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_FAILED)

    def test_consumer_will_retry_after_task_error(self):
        """Consumer will catch the tasks error and retry to run the task
        later.
        """
        task = create_task(function_name="tests.fixtures.failing", max_retries=3)

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_QUEUED)
        self.assertEqual(task.retries, 1)

    def test_consumer_will_retry_at_most_max_retries_times(self):
        """Consumer will not retry the task more than task.max_retries times."""
        task = create_task(function_name="tests.fixtures.failing", max_retries=2)

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

    def test_consumer_task_timeout(self):
        """Consumer will abort a task if it exceeds the timeout."""
        task = create_task(
            function_name="tests.fixtures.never_return", timeout=timedelta(seconds=2)
        )

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_FAILED)

    def test_consumer_db_error(self):
        task_count = 3
        assert Task.objects.count() == 0

        tasks = [create_task() for _ in range(task_count)]

        with self.assertLogs("taskq", level="ERROR") as taskq_error_logger_check:
            with patch.object(Task, "save", autospec=True) as mock_task_save:
                running_tasks = set()
                error_task = None

                def raise_if_last_running_task(self):
                    nonlocal error_task
                    print(f"MOCK SAVE {self.uuid} {self.get_status_display()}")
                    if self.status == Task.STATUS_RUNNING:
                        running_tasks.add(self.uuid)
                        if len(running_tasks) == len(tasks):
                            print(
                                f"!!! DB ERROR !!! {self.uuid} {self.get_status_display()}"
                            )
                            error_task = self
                            raise OperationalError()
                    return Model.save(self)

                mock_task_save.side_effect = raise_if_last_running_task

                consumer = Consumer()
                consumer.execute_tasks()

            assert Task.objects.filter(status=Task.STATUS_SUCCESS).count() == 2
            assert Task.objects.filter(status=Task.STATUS_FETCHED).count() == 1
            log_output = "\n".join(taskq_error_logger_check.output)
            assert "DB error" in log_output
            assert error_task.uuid in log_output

    def test_consumer_logs_cleaned_backtrace(self):
        """Consumer will log catched exceptions with internal frames removed
        from the backtrace."""
        create_task(function_name="tests.fixtures.failing_alphabet", max_retries=0)

        consumer = Consumer()

        with self.assertLogs("taskq", level="ERROR") as context_manager:
            consumer.execute_tasks()
            output = "".join(context_manager.output)
            lines = output.splitlines()

        # First line is our custom message
        self.assertIn("ValueError", lines[0])
        self.assertIn('I don\'t know what comes after "d"', lines[0])

        # Skip the first line (our message) and the second one ("Traceback
        # (most recent call last)")
        lines = lines[2:]

        # Only check the even lines (containing the filename, line and function name)
        relevant_lines = [l for i, l in enumerate(lines) if i % 2 == 0]

        # Check that we are getting the expected function names in the traceback
        expected_functions = [
            "_protected_call",
            "__call__",
            "failing_alphabet",
            "a",
            "b",
            "c",
            "d",
        ]
        for i, expected_function in enumerate(expected_functions):
            self.assertIn(expected_function, relevant_lines[i])

    @override_settings(TASKQ_FETCHED_TASKS_COUNT_LOGGED_AS_ERROR_THRESHOLD=4)
    def test_consumer_taskq_fetched_tasks_count_logging_threshold(self):
        consumer = Consumer()
        for i in range(3):
            create_task()

        with self.assertRaises(AssertionError):
            with self.assertLogs("taskq", ERROR):
                consumer.execute_tasks()

        for i in range(4):
            create_task()

        with self.assertLogs("taskq", ERROR) as log_check:
            consumer.execute_tasks()
            assert log_check.output
            assert "more than 4 tasks fetched" in "\n".join(log_check.output)

    @override_settings(
        TASKQ_FETCHED_TASKS_COUNT_LOGGED_AS_ERROR_THRESHOLD=3,
        TASKQ_FETCHED_TASKS_COUNT_ABOVE_ERROR_THRESHOLD_COUNTER_TRIGGER=3,
    )
    def test_consumer_taskq_fetched_tasks_count_logging_threshold_counter_trigger(self):
        consumer = Consumer()
        for i in range(3):
            create_task()

        with self.assertRaises(AssertionError):
            with self.assertLogs("taskq", ERROR):
                consumer.execute_tasks()

        for i in range(3):
            create_task()

        with self.assertRaises(AssertionError):
            with self.assertLogs("taskq", ERROR):
                consumer.execute_tasks()

        for i in range(3):
            create_task()

        with self.assertLogs("taskq", ERROR) as log_check:
            consumer.execute_tasks()
            assert log_check.output
            assert "more than 3 tasks fetched" in "\n".join(log_check.output)

    @override_settings(
        TASKQ_FETCHED_TASKS_COUNT_LOGGED_AS_ERROR_THRESHOLD=3,
        TASKQ_FETCHED_TASKS_COUNT_ABOVE_ERROR_THRESHOLD_COUNTER_TRIGGER=3,
    )
    def test_consumer_taskq_fetched_tasks_count_logging_threshold_counter_reset(self):
        consumer = Consumer()
        for i in range(3):
            create_task()

        with self.assertRaises(AssertionError):
            with self.assertLogs("taskq", ERROR):
                consumer.execute_tasks()

        for i in range(3):
            create_task()

        with self.assertRaises(AssertionError):
            with self.assertLogs("taskq", ERROR):
                consumer.execute_tasks()

        for i in range(2):
            create_task()

        with self.assertRaises(AssertionError):
            with self.assertLogs("taskq", ERROR):
                consumer.execute_tasks()

        # counter is reset
        assert consumer._fetched_tasks_count_above_error_threshold_counter == 0

        for i in range(3):
            create_task()

        with self.assertRaises(AssertionError):
            with self.assertLogs("taskq", ERROR):
                consumer.execute_tasks()

    @override_settings(
        TASKQ={
            "schedule": {
                "my-scheduled-task": {
                    "task": "tests.fixtures.do_nothing",
                    "cron": "0 1 * * *",  # crontab(minute=0, hour=1)
                }
            }
        }
    )
    def test_consumer_create_task_for_due_scheduled_task(self):
        """Consumer creates tasks for each scheduled task defined in settings."""
        consumer = Consumer()

        # Hack the due_at date to simulate the fact that the task was run once
        # already
        assert len(consumer._scheduler._tasks) == 1
        scheduled_task = consumer._scheduler._tasks[0]
        scheduled_task.due_at -= timedelta(days=1)

        consumer.create_scheduled_tasks()

        queued_tasks = Task.objects.filter(status=Task.STATUS_QUEUED).count()
        self.assertEqual(queued_tasks, 1)

    def test_consumer_logs_task_started(self):
        """Consumer will log that a task has started."""
        due_at = now() - timedelta(milliseconds=100)
        task = create_task(due_at=due_at)

        consumer = Consumer()
        with self.assertLogs("taskq", level="INFO") as context_manager:
            consumer.execute_tasks()
            output = "".join(context_manager.output)

        self.assertIn(task.uuid, output)
        self.assertIn("Started", output)

    def test_consumer_logs_task_started_nth_rety(self):
        """Consumer will log that a task has started and is executing its nth rety."""
        due_at = now() - timedelta(milliseconds=100)
        task = create_task(function_name="tests.fixtures.failing", due_at=due_at)

        consumer = Consumer()
        consumer.execute_tasks()

        with self.assertLogs("taskq", level="INFO") as context_manager:
            consumer.execute_tasks()
            output = "".join(context_manager.output)

        self.assertIn(task.uuid, output)
        self.assertIn("Started (1st retry)", output)
