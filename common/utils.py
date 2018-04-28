import json
from datetime import datetime


def time_passed(start_time):
    seconds_passed = (datetime.now() - start_time).total_seconds()
    if seconds_passed < 10:
        milliseconds_passed = round(seconds_passed * 1000)
        return '{}ms'.format(str(milliseconds_passed))
    else:
        seconds_passed = round(seconds_passed)
        return '{}s'.format(str(seconds_passed))


def format_size(n_bytes):
    if n_bytes > 2**30:
        n_gb = n_bytes / 2**30
        return '{} GB'.format(n_gb)
    elif n_bytes > 2**20:
        n_mb = n_bytes / 2**20
        return '{} MB'.format(n_mb)
    elif n_bytes > 2**10:
        n_kb = n_bytes / 2**10
        return '{} kB'.format(n_kb)
    else:
        return '{} bytes'.format(n_bytes)


def get_key_from_params(*args):
    params = [arg for arg in args if arg is not None]
    new_key = json.dumps(params)
    return new_key
