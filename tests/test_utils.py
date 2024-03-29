import datetime

from django.test import TransactionTestCase

from taskq.utils import parse_timedelta, ordinal


class UtilsParseTimedeltaTestCase(TransactionTestCase):
    def test_parse_timedelta_returns_input_if_timedelta(self):
        """parse_timedelta returns the passer input parameter if it is already
        a timedelta object.
        """
        in_obj = datetime.timedelta(hours=3, minutes=34)
        out_obj = parse_timedelta(in_obj)
        self.assertEqual(in_obj, out_obj)

    def test_parse_timedelta_converts_int_to_timedelta_seconds(self):
        """parse_timedelta returns a timedelta initialized with a number of
        seconds equals to its argument.
        """
        delay = parse_timedelta(59242)
        self.assertEqual(delay, datetime.timedelta(seconds=59242))

    def test_parse_timedelta_raises_for_unexpected_arg_types(self):
        """parse_timedelta raises a TypeError for input of a type which is not
        either datetime.timedelta or int.
        """
        self.assertRaises(TypeError, parse_timedelta, "Cheese?")
        self.assertRaises(
            TypeError, parse_timedelta, datetime.datetime(year=2000, month=4, day=20)
        )
        self.assertRaises(TypeError, parse_timedelta, [2, 45])


class UtilsOrdinalTestCase(TransactionTestCase):
    def test_ordinal_1(self):
        """ordinal(1) -> 1st"""
        self.assertEqual(ordinal(1), "1st")

    def test_ordinal_2(self):
        """ordinal(2) -> 2nd"""
        self.assertEqual(ordinal(2), "2nd")

    def test_ordinal_3(self):
        """ordinal(3) -> 3rd"""
        self.assertEqual(ordinal(3), "3rd")

    def test_ordinal_4(self):
        """ordinal(4) -> 4th"""
        self.assertEqual(ordinal(4), "4th")

    def test_ordinal_5(self):
        """ordinal(5) -> 5th"""
        self.assertEqual(ordinal(5), "5th")

    def test_ordinal_10(self):
        """ordinal(10) -> 10th"""
        self.assertEqual(ordinal(10), "10th")

    def test_ordinal_11(self):
        """ordinal(11) -> 11th"""
        self.assertEqual(ordinal(11), "11th")

    def test_ordinal_21(self):
        """ordinal(21) -> 21st"""
        self.assertEqual(ordinal(21), "21st")

    def test_ordinal_1250239(self):
        """ordinal(1250239) -> 1250239th"""
        self.assertEqual(ordinal(1250239), "1250239th")
