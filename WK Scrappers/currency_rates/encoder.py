# https://stackoverflow.com/questions/8793448/how-to-convert-to-a-python-datetime-object-with-json-loads

"""
To use a custom JSONDecoder subclass, specify it with the cls kwarg, otherwise JSONDecoder is used.
Additional keyword arguments will be passed to the constructor of the class.
"""

from json import JSONEncoder
import datetime


class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()
