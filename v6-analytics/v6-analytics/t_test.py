from vantage6.algorithm.tools.util import info
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.client import AlgorithmClient

import pandas as pd
import pandas.api.types as ptypes

from vantage6.algorithm.tools.util import info, error, get_env_var
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.tools.exceptions import InputError

from .decorator import new_data_decorator

T_TEST_MINIMUM_NUMBER_OF_RECORDS = 3


@algorithm_client
def t_test_central(
    client: AlgorithmClient, column_name: str, organizations_to_include: list[int]
) -> dict:
    """
    Send task to each node participating in the task to compute a local mean and sample
    variance, aggregate them to compute the t value for the independent sample t-test,
    and return the result.

    Parameters
    ----------
    client : AlgorithmClient
        The client object used to communicate with the server.
    column_name : str
        The column to compute the mean and sample variance for. The column must be
        numeric.
    organizations_to_include : list[int]
        The organizations to include in the task.

    Returns
    -------
    dict
        The `t` value for the independent-samples t test.
    """

    # Define input parameters for a subtask
    info("Defining input parameters")
    input_ = {
        "method": "t_test_partial",
        "kwargs": {
            "column_name": column_name,
        },
    }

    # create a subtask for all organizations in the collaboration.
    info("Creating subtask for all organizations in the collaboration")
    task = client.task.create(
        input_=input_,
        organizations=organizations_to_include,
        name="Subtask mean and sample variance",
        description="Compute mean and sample variance per data station.",
    )

    # wait for node to return results of the subtask.
    info("Waiting for results")
    results = client.wait_for_results(task_id=task.get("id"))
    info("Results obtained!")

    final_result = {}
    cohort_names = results[0].keys()
    for cohort_name in cohort_names:

        cohort_res = results[cohort_name]

        # Aggregate results to compute t value for the independent-samples t test
        # Compute pooled variance
        Sp = (
            (cohort_res[0]["count"] - 1) * cohort_res[0]["variance"]
            + (cohort_res[1]["count"] - 1) * cohort_res[1]["variance"]
        ) / (cohort_res[0]["count"] + cohort_res[1]["count"] - 2)

        # t value
        final_result[cohort_name] = (
            cohort_res[0]["average"] - cohort_res[1]["average"]
        ) / (((Sp / cohort_res[0]["count"]) + (Sp / cohort_res[1]["count"])) ** 0.5)

    # return the final results of the algorithm
    return final_result


@new_data_decorator
def t_test_partial(
    dfs: list[pd.DataFrame], cohort_names: list[str], column_name: str
) -> dict:
    results = {}
    for df, cohort_name in zip(dfs, cohort_names):
        results[cohort_name] = _t_test_partial(df, column_name)
    return results


def _t_test_partial(df: pd.DataFrame, column_name: str) -> dict:
    """
    Compute the mean and the sample variance of a column for a single data station to
    share with the aggregator part of the algorithm

    Parameters
    ----------
    df : pd.DataFrame
        The data for the data station
    column_name : str
        The column to compute the mean and sample variance for. The column must be
        numeric.

    Returns
    -------
    dict
        The mean, the number of observations and the sample variance for the data
        station.
    """

    info("Checking number of records in the DataFrame.")
    MINIMUM_NUMBER_OF_RECORDS = get_env_var(
        "T_TEST_MINIMUM_NUMBER_OF_RECORDS",
        T_TEST_MINIMUM_NUMBER_OF_RECORDS,
        as_type="int",
    )
    if len(df) <= MINIMUM_NUMBER_OF_RECORDS:
        raise InputError(
            "Number of records in 'df' must be greater than "
            f"{MINIMUM_NUMBER_OF_RECORDS}."
        )

    if not ptypes.is_numeric_dtype(df[column_name]):
        error("Column must be numeric.")

    ### Compute mean ----
    info("Computing mean")
    # Sum of the values
    column_sum = df[column_name].sum()
    # Count of observations
    count = len(df)
    # Mean
    average = column_sum / count

    ### Compute sample variance (S) ----
    info("Computing sample variance")
    # Sum of Squared Deviations (SSD)
    ssd = ((df[column_name].astype(float) - average) ** 2).sum()
    # Sample variance
    variance = ssd / (count - 1)

    # Return results to the vantage6 server.
    return {
        "average": float(average),
        "count": float(count),
        "variance": float(variance),
    }
