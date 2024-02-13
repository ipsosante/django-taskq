import datetime

from django.test import TransactionTestCase

from taskq.scheduler import ScheduledTask


class TaskFromScheduledTaskTestCase(TransactionTestCase):
    def test_can_create_task_from_scheduled_task(self):
        """task_from_scheduled_task creates a new Task from a ScheduledTask."""
        args = {"flour": 300, "pumpkin": True}
        scheduled_task = ScheduledTask(
            name="Cooking pie",
            task="kitchen.chef.cook_pie",
            cron="0 19 * * *",
            args=args,
            max_retries=1,
            retry_delay=22,
            retry_backoff=True,
            retry_backoff_factor=2,
        )

        task = scheduled_task.as_task
        self.assertIsNotNone(task)
        self.assertEqual(task.name, "Cooking pie")
        self.assertEqual(task.function_name, "kitchen.chef.cook_pie")
        self.assertEqual(task.function_args, {"flour": 300, "pumpkin": True})
        self.assertEqual(task.max_retries, 1)
        self.assertEqual(task.retry_delay, datetime.timedelta(seconds=22))
        self.assertEqual(task.retry_backoff, True)
        self.assertEqual(task.retry_backoff_factor, 2)
