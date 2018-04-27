from datetime import datetime
import pandas as pd

from common.utils import time_passed, get_key_from_params
from celery_worker.celery import app
from plasma_adapter.app import DataReader, DataWriter


@app.task
def add(x, y):
    return x + y


@app.task
def preload_data(filename, max_rows=None):
    writer = DataWriter()

    print('Worker: Loading large file into memory')
    start_time = datetime.now()
    df = pd.read_csv('data/{filename}'.format(filename=filename))
    print('Worker: Loaded file to dataframe in {}'.format(time_passed(start_time)))
    if max_rows is not None:
        df = df[0:max_rows]

    start_time = datetime.now()
    writer.cache_data_frame(df, filename)
    print('Worker: Wrote {} records to cache in {}'.format(str(len(df)), time_passed(start_time)))


@app.task
def filter_data_frame(key, filter_params):
    reader = DataReader()
    start_time = datetime.now()

    # TODO: Add a timeout so that we're not spending a lot of time trying to read a key that doesn't exist
    # It would be best if we could check for a key's existence before dispatching a task - then we can schedule a load
    df = reader.load_data_frame(key)
    print('Worker: Loaded data frame in {}'.format(time_passed(start_time)))

    to_filter = None
    for param in filter_params:
        col = param['column']
        value = param['value']
        operator = param['operator']

        if operator == '<':
            new_filter = df[col] < value
        elif operator == '>':
            new_filter = df[col] > value
        elif operator == '=':
            new_filter = df[col] == value
        else:
            new_filter = None

        if new_filter is not None:
            if to_filter is None:
                to_filter = new_filter
            else:
                to_filter = to_filter & new_filter

    start_time = datetime.now()
    print('Worker: Built filter in {}'.format(time_passed(start_time)))
    filtered_df = df[to_filter]

    start_time = datetime.now()
    print('Worker: Applied filter. Reduced from {} rows to {} in {}'.format(str(len(df)),
                                                                            str(len(filtered_df)),
                                                                            time_passed(start_time)))
    new_key = get_key_from_params([key, filter_params])

    writer = DataWriter()
    start_time = datetime.now()
    writer.cache_data_frame(filtered_df, new_key)
    print('Worker: Wrote filtered data frame back to object store in {}'.format(time_passed(start_time)))
