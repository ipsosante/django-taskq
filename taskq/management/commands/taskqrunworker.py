from django.core.management.base import BaseCommand

from taskq.consumer import Consumer


class Command(BaseCommand):
    """Start a new taskq Consumer, fetching and executing tasks as they are registered."""

    help = "Start a new task queue consumer"

    def handle(self, *args, **options):
        consumer = Consumer()
        consumer.run()
