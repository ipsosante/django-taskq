from django.core.management.base import BaseCommand

from taskq.consumer import Consumer

import logging
import sys


class Command(BaseCommand):
    """
    Queue worker.
    """

    help = "Run the queue consumer"

    def handle(self, *args, **options):

        logger = logging.getLogger('taskq')

        try:
            consumer = Consumer()
            consumer.run()
        except Exception as e:
            logger.exception('Taskq: ' + str(e), exc_info=sys.exc_info())
