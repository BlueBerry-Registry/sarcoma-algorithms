"""
This file contains all partial algorithm functions, that are normally executed
on all nodes for which the algorithm is executed.

The results in a return statement are sent to the vantage6 server (after
encryption if that is enabled). From there, they are sent to the partial task
or directly to the user (if they requested partial results).
"""

import os
import traceback
import pkg_resources
import pandas as pd
import numpy as np

from typing import Any

from rpy2.robjects import RS4
from rpy2.rinterface_lib.sexp import NACharacterType

from ohdsi.sqlrender import render, translate, read_sql
from ohdsi.database_connector import query_sql
from ohdsi.common import convert_from_r

from vantage6.algorithm.tools.util import info, error
from vantage6.algorithm.tools.decorators import (
    OHDSIMetaData,
    RunMetaData,
    metadata,
    database_connection,
)


@metadata
@database_connection(types=["OMOP"], include_metadata=True)
def partial(
    connection: RS4,
    meta_omop: OHDSIMetaData,
    meta_run: RunMetaData,
    database_label: str,
    cohort_id: float,
    cohort_task_id: int,
) -> Any:
    """
    Obtain the cohort from the database and store it over the CSV file.
    """
    info("Obtaining the cohort from the database")
    df = __create_cohort_dataframe(
        connection, meta_run, meta_omop, cohort_task_id, cohort_id
    )

    # Save the data to a CSV file
    info("Checking environment variables")
    csv_uri_env_var = f"{database_label.upper()}_DATABASE_URI"
    if csv_uri_env_var not in os.environ:
        error(f"Environment variable {csv_uri_env_var} not set")
        return {"error": "Environment variable not set"}

    info(f"Overwriting '{database_label}' CSV file")
    csv_uri = os.environ[csv_uri_env_var]
    df.to_csv(csv_uri, index=False)

    info("Done!")
    return {"msg": f"Overwritten '{database_label}' CSV file"}


def __create_cohort_dataframe(
    connection: RS4,
    meta_run: RunMetaData,
    meta_omop: OHDSIMetaData,
    cohort_task_id: int,
    shared_cohort_id: str,
) -> list[pd.DataFrame]:
    """
    Query the database for the data of the cohort.

    Parameters
    ----------
    connection : RS4
        Connection to the database.

    Returns
    -------
    pd.DataFrame
        The data of the cohort.
    """
    # Get the task id of the task that created the cohort at this node
    cohort_table = f"cohort_{cohort_task_id}_{meta_run.node_id}"
    info(f"Using cohort table: {cohort_table}")

    # Obtain the cohort IDs by combining the shared ids (equal over all nodes) with the
    # local node id
    cohort_id = float(f"{meta_run.node_id}{shared_cohort_id}")
    info(f"Using cohort ID: {cohort_id}")
    # TODO: Check if this is correct
    cohort_id = float(shared_cohort_id)

    # Obtain SQL file for standard features
    sql_path = pkg_resources.resource_filename(
        "v6-ohdsi-update-csv", "sql/standard_features.sql"
    )

    # SQL READ
    try:
        raw_sql = read_sql(sql_path)
    except Exception as e:
        error(f"Failed to read SQL file: {e}")
        traceback.print_exc()
        raise e
    info(f"Read SQL file: {sql_path}")

    info("Start query sequence the database")
    df = _query_database(connection, raw_sql, cohort_table, cohort_id, meta_omop)

    info("Post-processing the data")
    df["OBSERVATION_VAS"] = df["OBSERVATION_VAS"].apply(
        lambda val: np.nan if isinstance(val, NACharacterType) else val
    )

    # DROP DUPLICATES
    sub_df = df.drop_duplicates("SUBJECT_ID", keep="first")
    info(f"Dropped {len(df) - len(sub_df)} rows")

    return sub_df


def _query_database(
    connection: RS4,
    sql: str,
    cohort_table: str,
    cohort_id: float,
    meta_omop: OHDSIMetaData,
) -> pd.DataFrame:

    # RENDER
    info("Rendering the SQL")
    sql = render(
        sql,
        cohort_table=f"{meta_omop.results_schema}.{cohort_table}",
        cohort_id=cohort_id,
        cdm_database_schema=meta_omop.cdm_schema,
        incl_condition_concept_id=["NULL"],
        incl_procedure_concept_id=["NULL"],  # 4066543
        incl_measurement_concept_id=["NULL"],
        incl_drug_concept_id=["NULL"],  #'ALL' ? @TODO in algo
    )

    # TRANSLATE
    info("Translating the SQL")
    sql = translate(sql, target_dialect="postgresql")
    info(sql)

    # QUERY
    info("Querying the database")
    try:
        data_r = query_sql(connection, sql)
    except Exception as e:
        error(f"Failed to query the database: {e}")
        traceback.print_exc()
        with open("errorReportSql.txt", "r") as f:
            error(f.read())

    info("Convert to Python objects")
    return convert_from_r(data_r)
