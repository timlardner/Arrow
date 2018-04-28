import time
from common.utils import get_key_from_params
from celery_worker.filter import filter_data_frame, preload_data
from plasma_adapter.app import DataReader

FILENAME = 'midyear_population_age_country_code.csv'


def write_to_cache():
    result = preload_data.delay(FILENAME, max_rows=int(2e6), refresh=True)
    while not result.ready():
        time.sleep(1)


def dispatch_filter_task():
    # Create three different filters
    filters = [[{'column': 'country_code', 'value': 'CV', 'operator': '='}],

               [{'column': 'country_code', 'value': 'CV', 'operator': '='},
                {'column': 'year', 'value': 1993, 'operator': '='}],

               [{'column': 'country_code', 'value': 'CV', 'operator': '='},
                {'column': 'year', 'value': 1993, 'operator': '='},
                {'column': 'sex', 'value': 'Female', 'operator': '='}]]

    pagination = {'start': 0, 'end': 100}

    results = []
    for to_filter in filters:
        results.append(filter_data_frame.delay(FILENAME, to_filter, None, pagination))

    reader = DataReader()
    for result in results:
        while not result.ready():
            time.sleep(1)  # Ensure each process is complete

    for next_filter in filters:
        new_key = get_key_from_params([FILENAME, next_filter, None, pagination])
        df = reader.load_data_frame(new_key)
        if df is None:
            print('Data frame could not be found in cache!')
            continue

        print('Loaded data frame with {} records'.format(str(len(df))))


class FilterDataFrame:
    def __init__(self, key, filter_params=None, sort_params=None, pagination_params=None):
        self.result = None
        self.key = key
        self.filter = filter_params
        self.sort = sort_params
        self.pagination = pagination_params

    def execute(self):
        self.result = filter_data_frame.delay(self.key, self.filter, self.sort, self.pagination)


if __name__ == '__main__':
    write_to_cache()
    dispatch_filter_task()
