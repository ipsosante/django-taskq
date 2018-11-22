from django.test import TestCase

from taskq.consumer import Consumer

class ConsumerThreadingTestCase(TestCase):

    def test_consumer_can_stop(self):
        """Consumer can be stopped"""
        consumer = Consumer()
        consumer.start()
        self.assertTrue(consumer.is_alive())
        consumer.stop()
        consumer.join(timeout=1)
        self.assertFalse(consumer.is_alive())

        self.assertFalse(consumer.is_alive())
