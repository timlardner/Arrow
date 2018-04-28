from datetime import datetime
import pandas as pd

from celery.utils.log import get_task_logger

from common.utils import time_passed, get_key_from_params
from common.filter_tools import FilterTools
from celery_worker.celery import app
from plasma_adapter.app import DataReader, DataWriter

log = get_task_logger(__name__)


@app.task
def preload_data(filename, max_rows=None, refresh=False):
    writer = DataWriter()

    if writer.check_for_object(filename):
        log.warning('Worker: File {} is already in cache'.format(filename))
        if not refresh:
            return

    log.warning('Worker: Loading large file into memory')
    start_time = datetime.now()
    df = pd.read_csv('data/{filename}'.format(filename=filename))
    log.warning('Worker: Loaded file to data frame in {}'.format(time_passed(start_time)))
    if max_rows is not None:
        df = df[0:max_rows]

    start_time = datetime.now()
    writer.cache_data_frame(df, filename, force_eviction=True)
    log.warning('Worker: Wrote {} records to cache in {}'.format(str(len(df)), time_passed(start_time)))


@app.task
def filter_data_frame(key, filter_params, sort_params, pagination_params):
    reader = DataReader()
    start_time = datetime.now()

    new_key = get_key_from_params([key, filter_params, sort_params, pagination_params])

    if reader.check_for_object(new_key):
        log.warning('Worker: File with key {} is already in cache'.format(new_key))
        return

    df = reader.load_data_frame(key)
    if df is None:
        log.warning('Worker: Could not retrieve key from cache')
        return

    log.warning('Worker: Loaded data frame in {}'.format(time_passed(start_time)))

    if filter_params:
        df = FilterTools.filter(df, filter_params)

    if sort_params:
        df = FilterTools.sort(df, sort_params)

    if pagination_params:
        df = FilterTools.paginate(df, pagination_params)

    writer = DataWriter()
    start_time = datetime.now()
    writer.cache_data_frame(df, new_key)
    log.warning('Worker: Wrote processed data frame back to object store in {}'.format(time_passed(start_time)))
