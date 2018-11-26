import datetime

from django.test import TestCase

from taskq.utils import delay_timedelta


class UtilsDelayTimedeltaTestCase(TestCase):

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
