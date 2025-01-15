import re
import pandas as pd
import numpy as np

import pandas as pd

from typing import Dict, List, Union
from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.tools.util import info, error
from vantage6.algorithm.tools.decorators import algorithm_client
from vantage6.algorithm.tools.exceptions import PrivacyThresholdViolation

from typing import List
from vantage6.algorithm.tools.util import get_env_var, info, warn, error
from vantage6.algorithm.tools.decorators import data
from vantage6.algorithm.tools.exceptions import InputError, EnvironmentVariableError

from enum import Enum
from vantage6.algorithm.tools.util import get_env_var

from .decorator import new_data_decorator

# The following global variables are algorithm settings. They can be overwritten by
# the node admin by setting the corresponding environment variables.


KAPLAN_MEIER_MINIMUM_ORGANIZATIONS = 1

KAPLAN_MEIER_MINIMUM_NUMBER_OF_RECORDS = 3

KAPLAN_MEIER_ALLOWED_EVENT_TIME_COLUMNS_REGEX = ".*"

# Default noise type for event counts. Can be either "POISSON" or "GAUSSIAN".
KAPLAN_MEIER_TYPE_NOISE = "POISSON"

# Default gaussian noise SNR for event times, not that by default Poisson noise is
# used for event counts.
KAPLAN_MEIER_PRIVACY_SNR_EVENT_TIME = 0.0


class NoiseType(str, Enum):
    NONE = "NONE"
    GAUSSIAN = "GAUSSIAN"
    POISSON = "POISSON"


@new_data_decorator
def get_unique_event_times(
    dfs: list[pd.DataFrame], cohort_names: list[str], time_column_name: str
) -> List[List[str]]:
    results = {}
    for df, name in zip(dfs, cohort_names):
        results[name] = _get_unique_event_times(df, time_column_name)
    return results


@new_data_decorator
def get_km_event_table(
    dfs: list[pd.DataFrame],
    cohort_names: list[str],
    time_column_name: str,
    censor_column_name: str,
    unique_event_times: List[List[int | float]],
) -> List[str]:
    results = {}
    for df, name in zip(dfs, cohort_names):
        results[name] = _get_km_event_table(
            df, time_column_name, censor_column_name, unique_event_times[name]
        )
    return results


@algorithm_client
def kaplan_meier_central(
    client: AlgorithmClient,
    time_column_name: str,
    censor_column_name: str,
    organizations_to_include: List[int] | None = None,
) -> Dict[str, Union[str, List[str]]]:
    """
    Central part of the Federated Kaplan-Meier curve computation.

    This part is responsible for the orchestration and aggregation of the federated
    computation. The algorithm is executed in two steps on the nodes. The first step
    collects all unique event times from the nodes. The second step calculates the
    Kaplan-Meier curve and local event tables.

    Parameters
    ----------
    client : Vantage6 client object
        The client object used for communication with the server.
    time_column_name : str
        Name of the column containing the survival times.
    censor_column_name : str
        Name of the column containing the censoring.
    organizations_to_include : list of int, optional
        List of organization IDs to include (default: None, includes all).

    Returns
    -------
    dict
        Dictionary containing Kaplan-Meier curve and local event tables.
    """
    if not organizations_to_include:
        info("Collecting participating organizations")
        organizations_to_include = [
            organization.get("id") for organization in client.organization.list()
        ]

    MINIMUM_ORGANIZATIONS = get_env_var_as_int(
        "KAPLAN_MEIER_MINIMUM_ORGANIZATIONS", KAPLAN_MEIER_MINIMUM_ORGANIZATIONS
    )
    if len(organizations_to_include) < MINIMUM_ORGANIZATIONS:
        raise PrivacyThresholdViolation(
            "Minimum number of organizations not met, should be at least "
            f"{MINIMUM_ORGANIZATIONS}."
        )

    info("Collecting unique event times")
    local_unique_event_times_per_node = _start_partial_and_collect_results(
        client=client,
        method="get_unique_event_times",
        organizations_to_include=organizations_to_include,
        time_column_name=time_column_name,
    )

    info("Aggregating unique event times per cohort")
    all_unique_event_times = dict()
    cohort_names = local_unique_event_times_per_node[0].keys()
    for cohort_name in cohort_names:
        cohort_results = [res[cohort_name] for res in local_unique_event_times_per_node]

        unique_event_times = set()
        for local_unique_event_times in cohort_results:
            unique_event_times |= set(local_unique_event_times)

        all_unique_event_times[cohort_name] = list(unique_event_times)

    info("Collecting Kaplan-Meier curve and local event tables")
    local_km_per_node = _start_partial_and_collect_results(
        client=client,
        method="get_km_event_table",
        organizations_to_include=organizations_to_include,
        unique_event_times=all_unique_event_times,
        time_column_name=time_column_name,
        censor_column_name=censor_column_name,
    )

    info("Aggregating event tables")
    kaplan_meier_results = dict()
    for cohort_name in cohort_names:
        cohort_local_km_tables = [res[cohort_name] for res in local_km_per_node]
        local_event_tables = [
            pd.read_json(event_table) for event_table in cohort_local_km_tables
        ]

        km = (
            pd.concat(local_event_tables)
            .groupby(time_column_name, as_index=False)
            .sum()
        )
        km["hazard"] = km["observed"] / km["at_risk"]
        km["survival_cdf"] = (1 - km["hazard"]).cumprod()

        kaplan_meier_results[cohort_name] = km.to_json()

    info("Kaplan-Meier curve computed for all cohorts")
    return kaplan_meier_results


def _start_partial_and_collect_results(
    client: AlgorithmClient, method: str, organizations_to_include: List[int], **kwargs
) -> List[Dict[str, Union[str, List[str]]]]:
    """
    Launches a partial task to multiple organizations and collects their results when
    ready.

    Parameters
    ----------
    client : AlgorithmClient
        The vantage6 client used for communication with the server.
    method : str
        The method/function to be executed as a subtask by the organizations.
    organization_ids : List[int]
        A list of organization IDs to which the subtask will be distributed.
    **kwargs : dict
        Additional keyword arguments to be passed to the method/function.

    Returns
    -------
    List[Dict[str, Union[str, List[str]]]]
        A list of dictionaries containing results obtained from the organizations.
    """
    info(f"Including {len(organizations_to_include)} organizations in the analysis")
    task = client.task.create(
        input_={"method": method, "kwargs": kwargs},
        organizations=organizations_to_include,
    )

    info("Waiting for results")
    results = client.wait_for_results(task_id=task["id"])
    info(f"Results obtained for {method}!")
    return results


# FIXME: FM 22-05-2024 This function will be released with vantage6 4.5.0, and can be
#   removed from the algorithm code at that time.
def get_env_var_as_int(envvar_name: str, default: str) -> int:
    """
    Convert an environment variable to an integer value.

    Parameters
    ----------
    envvar_name : str
        The environment variable name to convert.
    default : str
        The default value to use if the environment variable is not set.

    Returns
    -------
    int
        The integer value of the environment variable.
    """
    envvar = get_env_var(envvar_name, default)
    error_msg = (
        f"Environment variable '{envvar_name}' has value '{envvar}' which cannot be "
        "converted to a positive integer value."
    )
    try:
        envvar = int(envvar)
    except ValueError as exc:
        raise ValueError(error_msg) from exc
    return envvar


def get_env_var_as_float(envvar_name: str, default: str) -> float:
    """
    Convert an environment variable to a float value.

    Parameters
    ----------
    envvar_name : str
        The environment variable name to convert.
    default : str
        The default value to use if the environment variable is not set.

    Returns
    -------
    float
        The float value of the environment variable.
    """
    envvar = get_env_var(envvar_name, default)
    error_msg = (
        f"Environment variable '{envvar_name}' has value '{envvar}' which cannot be "
        "converted to a float value."
    )
    try:
        envvar = float(envvar)
    except ValueError as exc:
        raise ValueError(error_msg) from exc
    return envvar


def get_env_var_as_list(envvar_name: str, default: str, separator: str = ",") -> list:
    """
    Convert an environment variable to a list. The environment variable should be a
    string with elements separated by a separator. The default value is used if the
    environment variable is not set.

    Parameters
    ----------
    envvar_name : str
        The environment variable name to convert.
    default : str
        The default value to use if the environment variable is not set.
    separator : str, optional
        The separator to use to split the environment variable (default: ',').

    Returns
    -------
    list
        The list of the environment variable.
    """
    envvar = get_env_var(envvar_name, default)
    return envvar.split(separator)


# TODO open PR on the original repository to add this function
def _get_unique_event_times(df: pd.DataFrame, time_column_name: str) -> List[str]:
    """
    Get unique event times from a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame supplied by the node.
    time_column_name : str
        Name of the column representing time.

    Returns
    -------
    List[str]
        List of unique event times.

    Raises
    ------
    InputError
        If the time column is not found in the DataFrame.
    """
    info("Getting unique event times.")
    info(f"Time column name: {time_column_name}.")
    info("Checking privacy guards.")
    _privacy_gaurds(df, time_column_name)

    df = _add_noise_to_event_times(df, time_column_name)

    return df[time_column_name].unique().tolist()


# TODO open PR on the original repository to add this function
def _get_km_event_table(
    df: pd.DataFrame,
    time_column_name: str,
    censor_column_name: str,
    unique_event_times: List[int | float],
) -> str:
    """
    Calculate death counts, total counts, and at-risk counts at each unique event time.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    time_column_name : str
        Name of the column representing time.
    censor_column_name : str
        Name of the column representing censoring.
    unique_event_times : List[int | float]
        List of unique event times.

    Returns
    -------
    str
        The Kaplan-Meier event table as a JSON string.
    """
    info("Checking privacy guards.")
    _privacy_gaurds(df, time_column_name)

    df = _add_noise_to_event_times(df, time_column_name)

    # Make sure the censor column is boolean
    # TODO this is a fix for the current implementation.. We use category and numberical
    # data types. Thus we need to convert it before we can use arithmetic operations.
    df[censor_column_name] = df[censor_column_name].astype(int)

    # Group by the time column, aggregating both death and total counts simultaneously
    km_df = (
        df.groupby(time_column_name)
        .agg(
            removed=(censor_column_name, "count"), observed=(censor_column_name, "sum")
        )
        .reset_index()
    )
    km_df["censored"] = km_df["removed"] - km_df["observed"]

    # Make sure all global times are available and sort it by time
    km_df = pd.merge(
        pd.DataFrame({time_column_name: unique_event_times}),
        km_df,
        on=time_column_name,
        how="left",
    ).fillna(0)
    km_df.sort_values(by=time_column_name, inplace=True)

    # Calculate "at-risk" counts at each unique event time
    km_df["at_risk"] = km_df["removed"].iloc[::-1].cumsum().iloc[::-1]

    # Convert DataFrame to JSON
    return km_df.to_json()


def _privacy_gaurds(df: pd.DataFrame, time_column_name: str) -> pd.DataFrame:
    """
    Check if the input data is valid and apply privacy guards.
    """

    info("Checking number of records in the DataFrame.")
    MINIMUM_NUMBER_OF_RECORDS = get_env_var_as_int(
        "KAPLAN_MEIER_MINIMUM_NUMBER_OF_RECORDS", KAPLAN_MEIER_MINIMUM_NUMBER_OF_RECORDS
    )
    if len(df) <= MINIMUM_NUMBER_OF_RECORDS:
        raise InputError(
            "Number of records in 'df' must be greater than "
            f"{MINIMUM_NUMBER_OF_RECORDS}."
        )

    info("Check that the selected time column is allowed by the node")
    ALLOWED_EVENT_TIME_COLUMNS_REGEX = get_env_var_as_list(
        "KAPLAN_MEIER_EVENT_TIME_COLUMN", KAPLAN_MEIER_ALLOWED_EVENT_TIME_COLUMNS_REGEX
    )
    for pattern in ALLOWED_EVENT_TIME_COLUMNS_REGEX:
        if re.match(pattern, time_column_name):
            break
    else:
        info(f"Allowed event time columns: {ALLOWED_EVENT_TIME_COLUMNS_REGEX}")
        raise InputError(
            f"Column '{time_column_name}' is not allowed as a time column."
        )

    if time_column_name not in df.columns:
        raise InputError(f"Column '{time_column_name}' not found in the data frame.")


def _add_noise_to_event_times(df: pd.DataFrame, time_column_name: str) -> pd.DataFrame:
    """
    Add noise to the event times in a DataFrame when this is requisted by the data-
    station.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame which contains the ``time_column_name`` column.
    time_column_name : str
        Privacy sensitive column name to which noise is going to b.

    Returns
    -------
    pd.DataFrame
        The DataFrame with added noise to the ``time_column_name``.
    """
    NOISE_TYPE = get_env_var("KAPLAN_MEIER_TYPE_NOISE", KAPLAN_MEIER_TYPE_NOISE).upper()
    if NOISE_TYPE == NoiseType.NONE:
        info("No noise is applied to the event times.")
        return df
    if NOISE_TYPE == NoiseType.GAUSSIAN:
        info("Gaussian noise is added to the event times.")
        return __apply_gaussian_noise(df, time_column_name)
    elif NOISE_TYPE == NoiseType.POISSON:
        info("Poisson noise is applied to the event times.")
        return __apply_poisson_noise(df, time_column_name)
    else:
        raise EnvironmentVariableError(f"Invalid noise type: {NOISE_TYPE}")


def __apply_gaussian_noise(df: pd.DataFrame, time_column_name: str) -> pd.DataFrame:
    """
    Apply Gaussian noise to the event times in a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        The DataFrame with Gaussian noise applied to the event times column.
    """
    # The signal-to-noise ratio (SNR) is used to determine the amount of noise to add.
    # First the variance of the time column is calculated. Then the standard deviation
    # of the noise is calculated by dividing the variance by the SNR. Finally the noise
    # is generated using a normal distribution with a mean of 0 and the calculated
    # standard deviation.
    #
    #  noise = N(0, sqrt(var_time / SNR))
    #
    SNR = get_env_var_as_float(
        "KAPLAN_MEIER_PRIVACY_SNR_EVENT_TIME", KAPLAN_MEIER_PRIVACY_SNR_EVENT_TIME
    )
    var_time = np.var(df[time_column_name])
    standard_deviation_noise = np.sqrt(var_time / SNR)
    __fix_random_seed()
    noise = np.round(np.random.normal(0, standard_deviation_noise, len(df)))

    # Add the noise to the time event column and clip the values to be non-negative as
    # negative event times do not make sense.
    df[time_column_name] += noise
    df[time_column_name] = df[time_column_name].clip(lower=0.0)
    info("Gaussion noise applied to the event times.")
    info(f"Variance of the time column: {var_time}")
    info(f"Standard deviation of the noise: {standard_deviation_noise}")
    return df


def __apply_poisson_noise(df: pd.DataFrame, time_column_name: str) -> pd.DataFrame:
    """
    Apply Poisson noise to the event times in a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        The DataFrame with Poisson noise applied to the event times column.
    """
    __fix_random_seed()

    # we can only apply noise to numerical values
    df.loc[df[time_column_name].notnull(), time_column_name] = np.random.poisson(
        df.loc[df[time_column_name].notnull(), time_column_name]
    )

    return df


def __fix_random_seed():
    """
    Every time before (every from the same function) a random number is generated we
    need to set the random seed to ensure reproducibility and privacy.
    """

    # In order to ensure that malicious parties can not reconstruct the orginal data
    # we need to add the same noise to the event times for every run. Else the party
    # can simply run the algorithm multiple times and average the results to get the
    # original event times.
    random_seed = get_env_var_as_int("KAPLAN_MEIER_RANDOM_SEED", "0")
    if random_seed == 0:
        warn(
            "Random seed is set to 0, this is not safe and should only be done for "
            "testing."
        )
    np.random.seed(random_seed)
