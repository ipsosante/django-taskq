import datetime
import json

from django.test import TestCase

from taskq.json import JSONEncoder, JSONDecoder


class JSONEncoderTestCase(TestCase):

    def test_json_encoding_integer(self):
        """JSONEncoder can encode integers."""
        value = 42
        json_repr = json.dumps(value, cls=JSONEncoder)

        self.assertEqual(json_repr, "42")

    def test_json_encoding_string(self):
        """JSONEncoder can encode strings."""
        value = "It's just a flesh wound."
        json_repr = json.dumps(value, cls=JSONEncoder)

        self.assertEqual(json_repr, "\"It's just a flesh wound.\"")

    def test_json_encoding_datetime(self):
        """JSONEncoder can encode datetimes."""
        value = datetime.datetime(year=1975, month=4, day=3,
                                  hour=14, minute=44, second=26)
        json_repr = json.dumps(value, cls=JSONEncoder)

        expected = ('{"__type__": "datetime", '
                    '"year": 1975, "month": 4, "day": 3, '
                    '"hour": 14, "minute": 44, "second": 26, '
                    '"microsecond": 0}')
        self.assertEqual(json_repr, expected)

    def test_json_encoding_timedelta(self):
        """JSONEncoder can encode timedeltas."""
        days = 3
        hours = 4
        minutes = 25
        seconds = 13
        value = datetime.timedelta(days=days, hours=hours, minutes=minutes,
                                   seconds=seconds)
        json_repr = json.dumps(value, cls=JSONEncoder)

        # timedelta only uses days, seconds and microseconds internally
        serialized_seconds = hours * 3600 + minutes * 60 + seconds
        expected = ('{"__type__": "timedelta", '
                    f'"days": 3, "seconds": {serialized_seconds}, '
                    '"microseconds": 0}')
        self.assertEqual(json_repr, expected)

    def test_json_encoding_unexpected_type(self):
        """JSONEncoder cannot encode arbitrary types and raises a TypeError."""
        class ArbitraryClass:
            def __init__(self):
                self.cheese = "Blue cheese"

        value = ArbitraryClass()
        self.assertRaises(TypeError, json.dumps, value, cls=JSONEncoder)


class JSONDecoderTestCase(TestCase):

    def test_json_decoding_integer(self):
        """JSONDecoder can decode integers."""
        json_value = "42"
        decoded = json.loads(json_value, cls=JSONDecoder)

        self.assertEqual(decoded, 42)

    def test_json_decoding_string(self):
        """JSONDecoder can decode strings."""
        json_value = "\"It's just a flesh wound.\""
        decoded = json.loads(json_value, cls=JSONDecoder)

        self.assertEqual(decoded, "It's just a flesh wound.")

    def test_json_decoding_datetime(self):
        """JSONDecoder can decode datetimes."""
        json_value = ('{"__type__": "datetime", '
                      '"year": 1971, "month": 9, "day": 28, '
                      '"hour": 20, "minute": 3, "second": 0, '
                      '"microsecond": 0}')
        decoded = json.loads(json_value, cls=JSONDecoder)

        expected = datetime.datetime(year=1971, month=9, day=28,
                                     hour=20, minute=3, second=0)
        self.assertEqual(decoded, expected)

    def test_json_decoding_timedelta(self):
        """JSONDecoder can decode timedeltas."""
        json_value = ('{"__type__": "timedelta", "days": 3, '
                      '"seconds": 14003, "microseconds": 100}')
        decoded = json.loads(json_value, cls=JSONDecoder)

        expected = datetime.timedelta(days=3, seconds=14003, microseconds=100)
        self.assertEqual(decoded, expected)

    def test_json_decoding_dict(self):
        """JSONDecoder can decode a regular dict."""
        json_value = '{"firstname": "Graham", "lastname": "Chapman"}'
        decoded = json.loads(json_value, cls=JSONDecoder)

        self.assertEqual(decoded['firstname'], 'Graham')
        self.assertEqual(decoded['lastname'], 'Chapman')

    def test_json_decoding_dict_with_type_key(self):
        """JSONDecoder can decode a dict with a __type__ key."""
        json_value = '{"__type__": "cheese", "holes": 6, "country": "France"}'
        decoded = json.loads(json_value, cls=JSONDecoder)

        self.assertEqual(decoded['__type__'], "cheese")
        self.assertEqual(decoded['holes'], 6)
        self.assertEqual(decoded['country'], "France")
