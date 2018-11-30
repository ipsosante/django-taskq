import datetime

from django.test import TransactionTestCase

from taskq.utils import delay_timedelta, task_from_scheduled_task
from taskq.scheduler import ScheduledTask


class UtilsDelayTimedeltaTestCase(TransactionTestCase):

    def test_delay_timedelta_returns_input_if_timedelta(self):
        """delay_timedelta returns the passer input parameter if it is already
        a timedelta object.
        """
        in_obj = datetime.timedelta(hours=3, minutes=34)
        out_obj = delay_timedelta(in_obj)
        self.assertEqual(in_obj, out_obj)

    def test_delay_timedelta_converts_int_to_timedelta_seconds(self):
        """delay_timedelta returns a timedelta initialized with a number of
        seconds equals to its argument.
        """
        delay = delay_timedelta(59242)
        self.assertEqual(delay, datetime.timedelta(seconds=59242))

    def test_delay_timedelta_raises_for_unexpected_arg_types(self):
        """delay_timedelta raises a TypeError for input of a type which is not
        either datetime.timedelta or int.
        """
        self.assertRaises(TypeError, delay_timedelta, "Cheese?")
        self.assertRaises(TypeError, delay_timedelta, datetime.datetime(
            year=2000, month=4, day=20
        ))
        self.assertRaises(TypeError, delay_timedelta, [2, 45])


class UtilsTaskFromScheduledTaskTestCase(TransactionTestCase):

    def test_can_create_task_from_scheduled_task(self):
        """task_from_scheduled_task creates a new Task from a ScheduledTask."""
        args = {
            'flour': 300,
            'pumpkin': True,
        }
        scheduled_task = ScheduledTask(
            name="Cooking pie", task="kitchen.chef.cook_pie", cron="0 19 * * *", args=args, max_retries=1,
            retry_delay=22, retry_backoff=True, retry_backoff_factor=2
        )

        task = task_from_scheduled_task(scheduled_task)
        self.assertIsNotNone(task)
        self.assertEqual(task.name, "Cooking pie")
        self.assertEqual(task.function_name, "kitchen.chef.cook_pie")
        self.assertEqual(task.function_args, '{"flour": 300, "pumpkin": true}')
        self.assertEqual(task.max_retries, 1)
        self.assertEqual(task.retry_delay, datetime.timedelta(seconds=22))
        self.assertEqual(task.retry_backoff, True)
        self.assertEqual(task.retry_backoff_factor, 2)
