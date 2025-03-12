import pandas as pd

from scipy import stats

from vantage6.algorithm.client import AlgorithmClient
from vantage6.algorithm.tools.decorators import algorithm_client

from .decorator import new_data_decorator


@algorithm_client
def crosstab_centers(
    client: AlgorithmClient,
    organizations_to_include: list[int] | None = None,
) -> tuple[dict, dict]:
    """ """
    if not organizations_to_include:
        organizations = client.organization.list()
        organizations_to_include = [
            organization.get("id") for organization in organizations
        ]

    # Get the data from the centers
    task = client.task.create(
        input_={
            "method": "compute_local_counts",
        },
        organizations=organizations_to_include,
        name="Crosstab centers subtask",
        description=f"Subtask to compute crosstab centers",
    )

    # Wait for the task to finish
    results = client.wait_for_results(task_id=task.get("id"))

    # Combine the results
    combined_df, chi_squared_df = combine_center_results(results)

    return combined_df, chi_squared_df


def combine_center_results(
    center_results: list[dict[str, list[dict[str, dict[str, int]]]]]
) -> tuple[dict, dict]:
    """
    Combine results from multiple centers and compute chi-squared tests.

    Parameters
    ----------
    center_results : list
        List of dictionaries containing counts from each center

    Returns
    -------
    tuple
        (combined_counts, chi_squared_results) where:
        - combined_counts: DataFrame with counts from all centers
        - chi_squared_results: DataFrame with chi-squared test results
    """
    # Combine counts
    rows = []
    # This is under the assumption that all cohorts have the same variables
    for cohort in center_results[0].keys():
        for var_dict in center_results[0][cohort]:
            var = list(var_dict.keys())[0]
            levels = var_dict[var].keys()
            for level in levels:
                rows.append(
                    {
                        "Cohort": cohort,
                        "Variable": var,
                        "Level": level,
                        **{
                            f"Center {i+1}": center[cohort][
                                center_results[0][cohort].index(var_dict)
                            ][var].get(level, 0)
                            for i, center in enumerate(center_results)
                        },
                    }
                )

    combined_df = pd.DataFrame(rows).sort_values(["Cohort", "Variable", "Level"])

    # Compute chi-squared tests
    center_cols = [col for col in combined_df.columns if col.startswith("Center")]
    results = []

    for cohort in combined_df["Cohort"].unique():
        cohort_data = combined_df[combined_df["Cohort"] == cohort]
        for var in cohort_data["Variable"].unique():
            var_data = cohort_data[cohort_data["Variable"] == var]
            contingency_table = var_data[center_cols].values

            # Check if the contingency table has any zero rows/columns
            if (contingency_table.sum(axis=0) > 0).all() and (
                contingency_table.sum(axis=1) > 0
            ).all():
                try:
                    chi2, p_val = stats.chi2_contingency(contingency_table)[:2]
                except ValueError:
                    chi2, p_val = None, None
            else:
                chi2, p_val = None, None

            results.append([cohort, var, chi2, p_val])

    chi_squared_df = pd.DataFrame(
        results, columns=["Cohort", "Variable", "Chi-squared", "P-value"]
    ).sort_values(["Cohort", "Variable"])

    return combined_df.to_json(), chi_squared_df.to_json()


@new_data_decorator
def compute_local_counts(
    dfs: list[pd.DataFrame], cohort_names: list[str]
) -> dict[str, list[dict[str, dict[str, int]]]]:
    """
    Compute local categorical value counts for each variable for multiple dataframes.

    Parameters
    ----------
    dfs : list[pandas.DataFrame]
        One or more dataframes containing the data
    cohort_names : list[str]
        Names of the cohorts corresponding to each dataframe

    Returns
    -------
    dict[str, list[dict[str, dict[str, int]]]]
        Nested dictionary with counts per cohort, variable and category level
        Structure:
        ```python
        {
            'cohort_name': [
                {'variable_name': {'level': count, ...}},
                ...
            ]
        }
        ```
    """
    if len(dfs) != len(cohort_names):
        raise ValueError("Number of dataframes must match number of cohort names")

    results = {}
    for df, cohort in zip(dfs, cohort_names):
        variables = df.select_dtypes(include=["category"]).columns
        results[cohort] = [{var: df[var].value_counts().to_dict()} for var in variables]
    return results
