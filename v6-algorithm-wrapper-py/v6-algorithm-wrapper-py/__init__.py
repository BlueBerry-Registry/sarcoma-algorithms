import vantage6.algorithm.tools.decorators
from functools import wraps
import pandas as pd
from vantage6.algorithm.tools.decorators import _get_user_database_labels

# TODO We should be able to compute all cohorts in one single go. This would make
# the exploration of the data much easier. We might need to do some fancy tricks in
# order to make the central function work as the output of the partial functions is
# not made to be iterable. Best option is probably to refactor the original code to
# have separate functions so we can rebuild the central function here without code
# duplication.
# TODO report the monkey path in the original repo
# TODO the dataframe types seem to be weird, maybe we should assign types during the
# extract process. Now everything is treated as a numerical value.


def new_data_decorator(_) -> callable:
    def protection_decorator(func: callable, *args, **kwargs) -> callable:
        @wraps(func)
        def decorator(
            *args, mock_data: list[pd.DataFrame] = None, **kwargs
        ) -> callable:
            cohort_name = _get_user_database_labels()[0]
            print(f"Using cohort {cohort_name}")
            data = pd.read_parquet(f"/mnt/data/{cohort_name}.parquet")
            print(data.head())
            args = (data, *args)
            return func(*args, **kwargs)

        decorator.wrapped_in_data_decorator = True
        return decorator

    return protection_decorator


vantage6.algorithm.tools.decorators.data = new_data_decorator
from importlib import import_module

_summary = import_module("v6-summary-py")
_summary.utils.DEFAULT_MINIMUM_ROWS = 0
_summary.utils.DEFAULT_PRIVACY_THRESHOLD = 0


def summary(*args, **kwargs):
    return _summary.summary(*args, **kwargs)


# Do not provide the columns as we want all columns to be included
def summary_per_data_station(*args, **kwargs) -> dict:
    return _summary.partial_summary.summary_per_data_station(*args, **kwargs)


def variance_per_data_station(*args, **kwargs) -> dict:
    return _summary.partial_variance.variance_per_data_station(*args, **kwargs)
