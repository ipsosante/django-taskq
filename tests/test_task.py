import datetime

from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from taskq.task import Taskify, taskify
from taskq.models import Task

from . import fixtures


class TaskifyDecoratorTestCase(TestCase):

    def test_taskify_sets_name(self):
        """The taskify decorator accepts a name parameter passes it to the
        created Taskify object.
        """
        def my_function():
            pass

        obj = taskify(my_function, name='MyGreatFunction')
        self.assertIsInstance(obj, Taskify)
        self.assertEqual(obj.name, 'MyGreatFunction')

    def test_taskify_name_defaults_to_function_name(self):
        """If no name parameter is passed to the taskify decorator the name
        of the Taskify object default to the "module.function" string.
        """
        def my_function():
            pass

        obj = taskify(my_function)
        self.assertIsInstance(obj, Taskify)
        self.assertEqual(obj.name, 'tests.test_task.my_function')

    def test_can_use_taskify_as_decorator_without_parenthesis(self):
        """The @taskify decorator can be applied without parenthesis."""
        self.assertIsInstance(fixtures.do_nothing, Taskify)

    def test_can_use_taskify_as_decorator_with_parenthesis(self):
        """The @taskify decorator can be applied with parenthesis (even without
        parameters).
        """
        self.assertIsInstance(fixtures.do_nothing_with_parenthesis, Taskify)


class TaskifyApplyTestCase(TestCase):

    def test_taskify_apply_simple_function(self):
        """A simple (no parameters) taskified function can be called with apply()."""
        self.assertEqual(fixtures.task_return_42.apply(), 42)

    def test_taskify_apply_with_positional_args(self):
        """A taskified function with positional args can be called with apply()."""
        self.assertEqual(fixtures.task_add.apply(4, 6), 10)

    def test_taskify_apply_with_kwargs(self):
        """A taskified function with kwargs can be called with apply()."""
        self.assertEqual(fixtures.task_divide.apply(21, b=3), 7)
        self.assertEqual(fixtures.task_divide.apply(b=5, a=0), 0)


class TaskifyApplyAsyncTestCase(TransactionTestCase):

    def test_taskify_apply_async_simple_function(self):
        """A task can be created from a simple (no parameters) taskified
        function with apply_async().
        """
        fixtures.task_return_42.apply_async()

        task = Task.objects.first()
        self.assertEqual(task.function_name, 'tests.fixtures.task_return_42')
        self.assertEqual(task.function_args, '{}')

    def test_taskify_apply_with_positional_args(self):
        """A task can be created from a taskified function with positional args
        with apply_async().
        """
        fixtures.task_add.apply_async(args=[4, 6])

        task = Task.objects.first()
        self.assertEqual(task.function_name, 'tests.fixtures.task_add')
        self.assertEqual(task.function_args, '{"__positional_args__": [4, 6]}')

    def test_taskify_apply_with_kwargs(self):
        """A task can be created from a taskified function with kwargs with
        apply_async()."""
        fixtures.task_divide.apply_async(kwargs={"b": 5, "a": 0})

        task = Task.objects.first()
        self.assertEqual(task.function_name, 'tests.fixtures.task_divide')
        self.assertEqual(task.function_args, '{"b": 5, "a": 0}')

    def test_taskify_apply_async_due_at_default_to_now(self):
        """A task created with apply_async() without an explicit due_at as its
        execution time defaulting to now.
        """
        before = timezone.now()
        fixtures.do_nothing.apply_async()
        after = timezone.now()

        task = Task.objects.first()
        self.assertGreaterEqual(task.due_at, before)
        self.assertLessEqual(task.due_at, after)

    def test_taskify_apply_async_due_at(self):
        """A task created with apply_async() uses its due_at parameter."""
        due_at = datetime.datetime(year=2032, month=6, day=13, hour=14, minute=7, tzinfo=timezone.utc)
        fixtures.do_nothing.apply_async(due_at=due_at)

        task = Task.objects.first()
        self.assertEqual(task.due_at, due_at)

    def test_taskify_apply_async_max_retries(self):
        """A task created with apply_async() uses its max_retries parameter."""
        fixtures.do_nothing.apply_async(max_retries=99)

        task = Task.objects.first()
        self.assertEqual(task.max_retries, 99)

    def test_taskify_apply_async_retry_delay_int(self):
        """A task created with apply_async() accepts an integer for its retry_delay parameter."""
        fixtures.do_nothing.apply_async(retry_delay=3600)

        task = Task.objects.first()
        self.assertEqual(task.retry_delay, datetime.timedelta(seconds=3600))

    def test_taskify_apply_async_retry_delay(self):
        """A task created with apply_async() accepts a timedelta for its retry_delay parameter."""
        delay = datetime.timedelta(hours=3, minutes=24)
        fixtures.do_nothing.apply_async(retry_delay=delay)

        task = Task.objects.first()
        self.assertEqual(task.retry_delay, datetime.timedelta(seconds=12240))

    def test_taskify_apply_async_retry_backoff(self):
        """A task created with apply_async() uses its retry_backoff parameter."""
        fixtures.do_nothing.apply_async(retry_backoff=True)
        task = Task.objects.first()
        self.assertTrue(task.retry_backoff)

        Task.objects.all().delete()  # Clean the db

        fixtures.do_nothing.apply_async(retry_backoff=False)
        task = Task.objects.first()
        self.assertFalse(task.retry_backoff)

    def test_taskify_apply_async_retry_backoff_factor(self):
        """A task created with apply_async() uses its retry_backoff_factor parameter."""
        fixtures.do_nothing.apply_async(retry_backoff_factor=77)

        task = Task.objects.first()
        self.assertEqual(task.retry_backoff_factor, 77)
