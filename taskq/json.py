import datetime
import json


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return {
                '__type__': 'datetime',
                'year': obj.year,
                'month': obj.month,
                'day': obj.day,
                'hour': obj.hour,
                'minute': obj.minute,
                'second': obj.second,
                'microsecond': obj.microsecond,
            }

        elif isinstance(obj, datetime.timedelta):
            return {
                '__type__': 'timedelta',
                'days': obj.days,
                'seconds': obj.seconds,
                'microseconds': obj.microseconds,
            }

        else:
            return json.JSONEncoder.default(self, obj)


class JSONDecoder(json.JSONDecoder):

    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object)

    def dict_to_object(self, d):
        if '__type__' not in d:
            return d

        obj_type = d.pop('__type__')
        if obj_type == 'datetime':
            return datetime.datetime(**d)
        elif obj_type == 'timedelta':
            return datetime.timedelta(**d)
        else:
            d['__type__'] = obj_type
            return d

# a = {
#     'date': datetime.datetime.now()
# }

# dump = json.dumps(a, cls=JSONEncoder)

# print(dump)

# print(json.loads(dump, cls=JSONDecoder))
