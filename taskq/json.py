import datetime
import json


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return self.default_datetime(obj)

        if isinstance(obj, datetime.timedelta):
            return self.default_timedelta(obj)

        return super().default(obj)

    def default_datetime(self, obj):
        return {
            "__type__": "datetime",
            "year": obj.year,
            "month": obj.month,
            "day": obj.day,
            "hour": obj.hour,
            "minute": obj.minute,
            "second": obj.second,
            "microsecond": obj.microsecond,
        }

    def default_timedelta(self, obj):
        return {
            "__type__": "timedelta",
            "days": obj.days,
            "seconds": obj.seconds,
            "microseconds": obj.microseconds,
        }


class JSONDecoder(json.JSONDecoder):
    def __init__(self):
        super().__init__(object_hook=self.dict_to_object)

    def dict_to_object(self, obj_dict):
        if "__type__" not in obj_dict:
            return obj_dict

        obj_type = obj_dict.pop("__type__")
        if obj_type == "datetime":
            return datetime.datetime(**obj_dict)
        if obj_type == "timedelta":
            return datetime.timedelta(**obj_dict)

        # Put __type__ back in the object dict
        obj_dict["__type__"] = obj_type
        return obj_dict
