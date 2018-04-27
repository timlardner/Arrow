import json
from datetime import datetime


def time_passed(startTime):
    seconds_passed = (datetime.now() - startTime).total_seconds()
    milliseconds_passed = seconds_passed * 1000
    return '{}ms'.format(str(milliseconds_passed))


def get_key_from_params(*args):
    params = list(args)
    new_key = json.dumps(params)
    return new_key