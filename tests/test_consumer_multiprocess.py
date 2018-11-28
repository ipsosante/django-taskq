import threading
from datetime import timedelta

from django.test import TransactionTestCase, override_settings
from django.utils.timezone import now

from taskq.models import Task

from .utils import create_task, create_background_consumers
from . import fixtures


class ConsumerMultiProcessTestCase(TransactionTestCase):

    def test_multiple_consumers_run_due_tasks_once(self):
        """Multiple Consumers running in parallel will execute each task only
        once.
        """
        fixtures.counter_reset()

        due_at = now() - timedelta(milliseconds=100)
        task = create_task(
            function_name='tests.fixtures.counter_increment',
            due_at=due_at
        )

        # Create consumers running in parallel
        consumers_count = 2
        barrier = threading.Barrier(consumers_count, timeout=5)
        consumers, threads = create_background_consumers(
            consumers_count,
            sleep_rate=0.1,
            execute_tasks_barrier=barrier
        )

        for consumer in consumers:
            consumer.stop()
        for thread in threads:
            thread.join(timeout=5)

        task.refresh_from_db()
        self.assertEqual(task.status, Task.STATUS_SUCCESS)
        self.assertEqual(fixtures.counter_get_value(), 1)

    @override_settings(TASKQ={
        'schedule': {
            'my-scheduled-task': {
                'task': 'tests.fixtures.do_nothing',
                'cron': '0 1 * * *',  # crontab(minute=0, hour=1)
            }
        }
    })
    def test_multiple_consumers_create_task_for_due_scheduled_task(self):
        # Create consumers running in parallel
        consumers_count = 2
        consumers, threads = create_background_consumers(
            consumers_count,
            auto_start=False,
            sleep_rate=0.1,
        )

        for consumer in consumers:
            # Hack the due_at date to simulate the fact that the task was run
            # once already.
            assert len(consumer._scheduler._tasks) == 1
            scheduled_task = consumer._scheduler._tasks[0]
            scheduled_task.due_at -= timedelta(days=1)

        for thread in threads:
            thread.start()
        for consumer in consumers:
            consumer.stop()
        for thread in threads:
            thread.join(timeout=5)

        queued_tasks = Task.objects.filter(status=Task.STATUS_SUCCESS).count()
        self.assertEqual(queued_tasks, 1)
