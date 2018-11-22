from datetime import timedelta

from django.test import TestCase
from django.utils.timezone import now

from taskq.consumer import Consumer
from taskq.models import Task

class ConsumerTestCase(TestCase):

    def test_consumer_can_stop(self):
        """Consumer can be stopped"""
        consumer = Consumer(sleep_rate=1)
        consumer.start()
        self.assertTrue(consumer.is_alive())
        consumer.stop()
        consumer.join(timeout=5)
        self.assertFalse(consumer.is_alive())

    def test_consumer_run_due_task(self):
        """Consumer will run task at or after their due date"""
        task = Task()
        task.due_at = now() - timedelta(milliseconds=100)
        task.function_name = 'tests.fixtures.do_nothing'
        task.function_args = '{}'
        task.save()

        consumer = Consumer()
        consumer.execute_tasks()

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_SUCCESS)
