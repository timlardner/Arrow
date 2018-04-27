import time
from common.utils import get_key_from_params
from celery_worker.filter import filter_data_frame, preload_data
from plasma_adapter.app import DataReader

FILENAME = 'midyear_population_age_country_code.csv'


def write_to_cache():
    result = preload_data.delay(FILENAME, max_rows=int(2e6))
    while not result.ready():
        time.sleep(1)


def dispatch_filter_task():
    filters = []
    filters.append([{'column': 'country_code', 'value': 'CV', 'operator': '='}])
    filters.append([{'column': 'country_code', 'value': 'CV', 'operator': '='}, {'column': 'year', 'value': 1993, 'operator': '='}])
    filters.append([{'column': 'country_code', 'value': 'CV', 'operator': '='}, {'column': 'year', 'value': 1993, 'operator': '='}, {'column': 'sex', 'value': 'Female', 'operator': '='}])

    results = []
    for to_filter in filters:
        results.append(filter_data_frame.delay(FILENAME, to_filter))

    reader = DataReader()
    for result in results:
        while not result.ready():
            time.sleep(1)  # Ensure each filter is complete

    for next_filter in filters:
        new_key = get_key_from_params([FILENAME, next_filter])
        df = reader.load_data_frame(new_key)
        print('Loaded data frame with {} records'.format(str(len(df))))


if __name__ == '__main__':
    write_to_cache()
    dispatch_filter_task()
