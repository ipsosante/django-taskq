from django.test import TransactionTestCase
from django.utils.timezone import now
from django.db.utils import IntegrityError
from django.core.exceptions import ValidationError

from taskq.models import Task


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

    def test_tasks_arguments_encoding_args(self):
        """The function positional args are properly encoded when using encode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.encode_function_args(['a', 1, 'foo', True])
        task.save()

        expected = '{"__positional_args__": ["a", 1, "foo", true]}'
        self.assertEqual(task.function_args, expected)

    def test_tasks_arguments_encoding_kwargs(self):
        """The function kwargs are properly encoded when using encode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.encode_function_args(kwargs={
            'cheese': 'blue',
            'fruits_count': 8
        })
        task.save()

        expected = '{"cheese": "blue", "fruits_count": 8}'
        self.assertEqual(task.function_args, expected)

    def test_tasks_arguments_encoding_mixed_args(self):
        """The function parameters are properly encoded when using encode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.encode_function_args(
            ['a', 'b', 42],
            {
                'cheese': 'blue',
                'fruits_count': 2
            }
        )
        task.save()

        expected = '{"cheese": "blue", "fruits_count": 2, "__positional_args__": ["a", "b", 42]}'
        self.assertEqual(task.function_args, expected)

    def test_tasks_arguments_decoding_args(self):
        """The function positional args are properly decoded when using decode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.function_args = '{"__positional_args__": [4, true, "banana"]}'
        task.save()

        expected = ([4, True, "banana"], {})
        self.assertEqual(task.decode_function_args(), expected)

    def test_tasks_arguments_decoding_kwargs(self):
        """The function kwargs are properly decoded when using decode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.function_args = '{"cheese": "blue", "fruits_count": 8}'
        task.save()

        expected = ([], {
            "cheese": "blue",
            "fruits_count": 8
        })
        self.assertEqual(task.decode_function_args(), expected)

    def test_tasks_arguments_decoding_mixed_args(self):
        """The function parameters are properly decoded when using decode_function_args()."""
        task = Task()
        task.due_at = now()
        task.function_name = "tests.fixtures.do_nothing"
        task.function_args = '{"cheese": "blue", "fruits_count": 8, "__positional_args__": [7, "orange"]}'
        task.save()

        expected = ([7, "orange"], {
            "cheese": "blue",
            "fruits_count": 8
        })
        self.assertEqual(task.decode_function_args(), expected)
