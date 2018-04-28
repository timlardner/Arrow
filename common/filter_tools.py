from datetime import datetime
from common.utils import time_passed


class FilterTools:
    @classmethod
    def filter(cls, df, filter_params):
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

        filtered_df = df[to_filter]

        start_time = datetime.now()
        print('FilterTools: Applied filter. Reduced from {} rows to {} in {}'.format(str(len(df)),
                                                                                     str(len(filtered_df)),
                                                                                     time_passed(start_time)))
        return filtered_df

    @classmethod
    def sort(cls, df, sort_params):
        start_time = datetime.now()

        columns = sort_params.get('columns')
        ascending = sort_params.get('ascending', True)

        df = df.sort_values(by=columns, ascending=ascending)

        print('FilterTools: Applied sort in {}'.format(time_passed(start_time)))
        return df

    @classmethod
    def paginate(cls, df, pagination_params):
        start_time = datetime.now()

        start = pagination_params.get('start', 0)
        end = pagination_params.get('end', -1)

        df = df[start:end]

        print('FilterTools: Paginated data frame in {}'.format(time_passed(start_time)))
        return df
