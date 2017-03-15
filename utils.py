
#!/usr/bin/env python

import json
import datetime

def get_dict_val(dict, key_name):
    """
    Receives the arguments = dict, key_name
    Returns the value
    """
    return dict.get(key_name)

def load_json(msg):
    """
    Load the message value to json.
    """
    return json.loads(msg)

def datetime_from_epoch(time):
    return datetime.datetime.utcfromtimestamp(float(time)/1000.0)


