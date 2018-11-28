from django.test import TestCase

from taskq.task import Taskify, taskify

from . import fixtures


class TaskifyDecoratorTestCase(TestCase):

    def test_taskify_sets_name(self):
        def my_function():
            pass

        obj = taskify(my_function, name='MyGreatFunction')
        self.assertIsInstance(obj, Taskify)
        self.assertEqual(obj.name, 'MyGreatFunction')

    def test_taskify_name_defaults_to_function_name(self):
        def my_function():
            pass

        obj = taskify(my_function)
        self.assertIsInstance(obj, Taskify)
        self.assertEqual(obj.name, 'tests.test_task.my_function')

    def test_can_use_taskify_as_decorator_without_parenthesis(self):
        self.assertIsInstance(fixtures.do_nothing, Taskify)

    def test_can_use_taskify_as_decorator_with_parenthesis(self):
        self.assertIsInstance(fixtures.do_nothing_with_parenthesis, Taskify)
