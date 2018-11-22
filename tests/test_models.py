from django.test import TestCase
from django.utils.timezone import now
from django.db.utils import IntegrityError
from django.core.exceptions import ValidationError

from taskq.models import Task

class TaskTestCase(TestCase):

    def test_cannot_create_task_without_function_name(self):
        """Tasks cannot be created without a function name"""
        task = Task()
        task.due_at = now()
        self.assertRaises(IntegrityError, task.save)

    def test_cannot_create_task_with_empty_function_name(self):
        """Tasks cannot be created with an empty function name"""
        task = Task()
        task.due_at = now()
        task.function_name = ""
        self.assertRaises(ValidationError, task.save)
