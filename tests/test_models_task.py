import datetime

from django.core.exceptions import ValidationError
from django.db.utils import IntegrityError
from django.test import TestCase, TransactionTestCase
from django.utils import timezone
from django.utils.timezone import now

from taskq.consumer import Consumer
from taskq.exceptions import TaskLoadingError
from taskq.models import Task
from tests.utils import create_task


class TaskTestCase(TransactionTestCase):
    def test_cannot_create_task_without_function_name(self):
        """Tasks cannot be created without a function name."""
        task = Task()
        task.due_at = now()
        self.assertRaises(IntegrityError, task.save)

    def test_cannot_create_task_with_empty_function_name(self):
        """Tasks cannot be created with an empty function name."""
        task = Task()
        task.due_at = now()
        task.function_name = ""
        self.assertRaises(ValidationError, task.save)

    def test_new_tasks_have_default_uuid(self):
        """Newly created tasks have an auto-assigned uuid."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.save()
        self.assertIsNotNone(task.uuid)
        self.assertEqual(len(task.uuid), 36)

    def test_tasks_repr_contains_uuid(self):
        """The string representation of a Task contains its UUID."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.save()
        self.assertTrue(task.uuid in str(task))

    def test_tasks_repr_contains_name_if_set(self):
        """The string representation of a Task contains its name if one was set."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.name = "Banana"
        task.save()
        self.assertTrue("Banana" in str(task))

    def test_tasks_repr_contains_status_string(self):
        """The string representation of a Task contains its status as a readable string."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.status = Task.STATUS_RUNNING
        task.save()
        self.assertTrue("Running" in str(task))

    def test_tasks_update_due_at_simple(self):
        """The task updates its due_at after an execution failure."""
        base_due_at = datetime.datetime(2018, 11, 28, hour=20, tzinfo=timezone.utc)

        task = Task()
        task.due_at = base_due_at
        task.retry_delay = datetime.timedelta(seconds=10)
        task.function_name = "tests.fixtures.do_nothing"
        task.save()
        self.assertEqual(task.due_at, base_due_at)

        previous_due_at = task.due_at
        for i in range(1, 4):
            task.retries = i
            before = timezone.now()
            task.update_due_at_after_failure()
            after = timezone.now()
            delta = datetime.timedelta(seconds=10)
            self.assertGreaterEqual(task.due_at, before + delta)
            self.assertLessEqual(task.due_at, after + delta)

            self.assertGreater(task.due_at, previous_due_at)
            previous_due_at = task.due_at

    def test_tasks_update_due_at_with_backoff(self):
        """The task updates its due_at with a quadratic growing delay if
        retry_backoff is true.
        """
        base_due_at = datetime.datetime(2018, 11, 28, hour=20, tzinfo=timezone.utc)

        task = Task()
        task.due_at = base_due_at
        task.retry_delay = datetime.timedelta(seconds=10)
        task.retry_backoff = True
        task.function_name = "tests.fixtures.do_nothing"
        task.save()
        self.assertEqual(task.due_at, base_due_at)

        previous_due_at = task.due_at
        for i in range(1, 4):
            task.retries = i
            before = timezone.now()
            task.update_due_at_after_failure()
            after = timezone.now()
            delta = datetime.timedelta(seconds=10 * (2 ** (i - 1)))
            self.assertGreaterEqual(task.due_at, before + delta)
            self.assertLessEqual(task.due_at, after + delta)

            self.assertGreater(task.due_at, previous_due_at)
            previous_due_at = task.due_at

    def test_tasks_update_due_at_with_custom_backoff_factor(self):
        """The task updates its due_at with a delay growing according to
        retry_backoff_factor set.
        """
        base_due_at = datetime.datetime(2018, 11, 28, hour=20, tzinfo=timezone.utc)

        task = Task()
        task.due_at = base_due_at
        task.retry_delay = datetime.timedelta(seconds=10)
        task.retry_backoff = True
        task.retry_backoff_factor = 4
        task.function_name = "tests.fixtures.do_nothing"
        task.save()
        self.assertEqual(task.due_at, base_due_at)

        previous_due_at = task.due_at
        for i in range(1, 4):
            task.retries = i
            before = timezone.now()
            task.update_due_at_after_failure()
            after = timezone.now()
            delta = datetime.timedelta(seconds=10 * (4 ** (i - 1)))
            self.assertGreaterEqual(task.due_at, before + delta)
            self.assertLessEqual(task.due_at, after + delta)

            self.assertGreater(task.due_at, previous_due_at)
            previous_due_at = task.due_at

    def test_tasks_update_due_at_ignores_backoff_factor_if_backoff_false(self):
        """The task updates its due_at without taking into account its
        retry_backoff_factor if retry_backoff is False.
        """
        base_due_at = datetime.datetime(2018, 11, 28, hour=20, tzinfo=timezone.utc)

        task = Task()
        task.due_at = base_due_at
        task.retry_delay = datetime.timedelta(seconds=10)
        task.retry_backoff = False
        task.retry_backoff_factor = 4
        task.function_name = "tests.fixtures.do_nothing"
        task.save()
        self.assertEqual(task.due_at, base_due_at)

        for i in range(1, 4):
            task.retries = i
            before = timezone.now()
            task.update_due_at_after_failure()
            after = timezone.now()
            delta = datetime.timedelta(seconds=10)
            self.assertGreaterEqual(task.due_at, before + delta)
            self.assertLessEqual(task.due_at, after + delta)

    def test_task_function_args_cannot_modified_by_task_function(self):
        original_args = {"__positional_args__": ["a"], "context": {"foo": "bar"}}
        task = create_task(
            function_name="tests.fixtures.task_update_context",
            function_args=original_args,
        )
        consumer = Consumer()
        consumer.process_task(task)
        task.refresh_from_db()
        assert task.function_args == original_args


class TaskFunctionArgsTestCase(TestCase):
    def test_tasks_arguments_encoding_args(self):
        """The function positional args are properly encoded when using encode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.encode_function_args(["a", 1, "foo", True])
        task.save()

        expected = {"__positional_args__": ["a", 1, "foo", True]}
        self.assertEqual(task.function_args, expected)

    def test_tasks_arguments_encoding_kwargs(self):
        """The function kwargs are properly encoded when using encode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.encode_function_args(kwargs={"cheese": "blue", "fruits_count": 8})
        task.save()

        expected = {"cheese": "blue", "fruits_count": 8}
        self.assertEqual(task.function_args, expected)

    def test_tasks_arguments_encoding_mixed_args(self):
        """The function parameters are properly encoded when using encode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.encode_function_args(["a", "b", 42], {"cheese": "blue", "fruits_count": 2})
        task.save()

        expected = {
            "cheese": "blue",
            "fruits_count": 2,
            "__positional_args__": ["a", "b", 42],
        }
        self.assertEqual(task.function_args, expected)

    def test_tasks_arguments_decoding_args(self):
        """The function positional args are properly decoded when using decode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.function_args = {"__positional_args__": [4, True, "banana"]}
        task.save()

        expected = ([4, True, "banana"], {})
        self.assertEqual(task.decode_function_args(), expected)

    def test_tasks_arguments_decoding_kwargs(self):
        """The function kwargs are properly decoded when using decode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.function_args = {"cheese": "blue", "fruits_count": 8}
        task.save()

        expected = ([], {"cheese": "blue", "fruits_count": 8})
        self.assertEqual(task.decode_function_args(), expected)

    def test_tasks_arguments_decoding_mixed_args(self):
        """The function parameters are properly decoded when using decode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.function_args = {
            "cheese": "blue",
            "fruits_count": 8,
            "__positional_args__": [7, "orange"],
        }
        task.save()

        expected = ([7, "orange"], {"cheese": "blue", "fruits_count": 8})
        self.assertEqual(task.decode_function_args(), expected)


class ImportTaskifiedFunctionTestCase(TransactionTestCase):
    def test_can_import_existing_task(self):
        """Consumer can import a valid and existing @taskified function."""

        func = Task.import_taskified_function("tests.fixtures.do_nothing")
        self.assertIsNotNone(func)

    def test_fails_import_non_taskified_functions(self):
        """Consumer raises when trying to import a function not decorated with
        @taskify.
        """
        self.assertRaises(
            TaskLoadingError,
            Task.import_taskified_function,
            "tests.fixtures.naked_function",
        )

    def test_fails_import_non_existing_module(self):
        """Consumer raises when trying to import a function from a non-existing
        module.
        """
        self.assertRaises(
            TaskLoadingError, Task.import_taskified_function, "tests.foobar.nope"
        )

    def test_fails_import_non_existing_function(self):
        """Consumer raises when trying to import a non-existing function."""
        self.assertRaises(
            TaskLoadingError,
            Task.import_taskified_function,
            "tests.fixtures.not_a_known_function",
        )

    def test_fails_import_function_syntax_error(self):
        """Consumer raises when trying to import a function with a Python
        syntax error.
        """
        self.assertRaises(
            TaskLoadingError,
            Task.import_taskified_function,
            "tests.fixtures_broken.broken_function",
        )
