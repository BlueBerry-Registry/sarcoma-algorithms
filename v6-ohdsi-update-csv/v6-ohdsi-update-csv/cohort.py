import traceback
import pkg_resources
import datetime

import pandas as pd
import numpy as np

from pathlib import Path

from rpy2.robjects import RS4
from rpy2.rinterface_lib.sexp import NACharacterType

from vantage6.algorithm.tools.util import info, error
from vantage6.algorithm.tools.decorators import (
    database_connection,
    OHDSIMetaData,
    metadata,
    RunMetaData,
)

from ohdsi import cohort_generator
from ohdsi import circe
from ohdsi import common as ohdsi_common
from ohdsi import sqlrender
from ohdsi import database_connector


def del_cohorts(cohort_names: list[str]):
    for cohort_name in cohort_names:
        Path(f"/mnt/data/{cohort_name}.parquet").unlink()
    return {"msg": f"Cohort(s) {', '.join(cohort_names)} deleted"}


@metadata
def get_cohorts(meta_run: RunMetaData):
    files = Path("/mnt/data").glob("cohort_*.parquet")
    # get the filenames, dates and number of records

    metadata = []
    for file_ in files:
        df = pd.read_parquet(file_)
        metadata.append(
            {
                "name": file_.name.split(".")[0],
                "created_at": datetime.datetime.fromtimestamp(
                    file_.stat().st_mtime
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "observations": df.shape[0],
                "variables": list(df.columns),
                "organization": meta_run.organization_id,
            }
        )

    return metadata


@metadata
@database_connection(types=["OMOP"], include_metadata=True)
def create_cohort(
    connection: RS4,
    meta_omop: OHDSIMetaData,
    meta_run: RunMetaData,
    cohort_definitions: list[dict],
    cohort_names: list[str],
):

    # The first step is to create the cohorts in result schema of the database. This
    # schema should have write permissions for the user that is used to connect to the
    # database.
    n = len(cohort_definitions)
    info(f"Creating {n} cohort(s) in the database")
    # To create a unique cohort table name, we use the task ID and index. The user does
    # not have to worry about this, as the data is retrieved from the database
    # immediately after creation.
    cohort_ids = []
    for i in range(0, n):
        cohort_ids.append(float(f"{meta_run.task_id:04d}{i:03d}"))

    # The next step is to create the tables in the database who are responsible for
    # storing the cohort data.
    cohort_table = f"cohort_{meta_run.task_id}_{meta_run.node_id}"
    cohort_table_names = cohort_generator.get_cohort_table_names(cohort_table)
    info(f"Cohort table name: {cohort_table}")
    info(f"Tables: {cohort_table_names}")

    cohort_generator.create_cohort_tables(
        connection=connection,
        cohort_database_schema=meta_omop.results_schema,
        cohort_table_names=cohort_table_names,
    )

    # Then we create a table with all cohort definitions and their corresponding SQL
    cohort_definition_set = pd.DataFrame(
        {
            "cohortId": cohort_ids,
            "cohortName": cohort_names,
            "json": cohort_definitions,
            "sql": [_create_cohort_query(cohort) for cohort in cohort_definitions],
            "logicDescription": [None] * n,
            "generateStats": [True] * n,
        }
    )
    cohort_definition_set = ohdsi_common.convert_to_r(cohort_definition_set)
    info(f"Generated {n} cohort definitions including SQL")

    info("Executing cohort generation")
    cohort_generator.generate_cohort_set(
        connection=connection,
        cdm_database_schema=meta_omop.cdm_schema,
        cohort_database_schema=meta_omop.results_schema,
        cohort_table_names=cohort_table_names,
        cohort_definition_set=cohort_definition_set,
    )
    info("Generated cohort set")

    info("Providing the cohort dataset to vantage6")
    for cohort_id, cohort_name in zip(cohort_ids, cohort_names):
        info(f"Retrieving variables for cohort: {cohort_id} {cohort_name}")
        try:
            df = __create_cohort_dataframe(
                connection, meta_omop, cohort_table, cohort_id
            )
        except Exception as e:
            error(f"Failed to create cohort dataframe: {cohort_name}, continuing")
            continue

        try:
            df.to_parquet(f"/mnt/data/cohort_{cohort_name}.parquet")
        except Exception as e:
            error(
                f"Failed to save cohort data to /mnt/data/cohort_{cohort_name}.parquet"
            )
            traceback.print_exc()
            return {
                "error": f"Failed to save cohort data to /mnt/data/cohort_{cohort_name}.parquet"
            }

        # TODO change this to a parquet file
        info(f"Saved cohort data to /mnt/data/cohort_{cohort_name}.parquet")

    # TODO clean up the results schema as we do not need it anymore
    info("Done!")
    return {"msg": "Cohort created and available for use on this node"}


def _create_cohort_query(cohort_definition: dict) -> str:
    """
    Creates a cohort query from a cohort definition in JSON format.

    Parameters
    ----------
    cohort_definition: dict
        The cohort definition in JSON format, for example created from ATLAS.

    Returns
    -------
    str
        The cohort query.
    """
    cohort_expression = circe.cohort_expression_from_json(cohort_definition)
    options = circe.create_generate_options(generate_stats=True)
    return circe.build_cohort_query(cohort_expression, options)[0]


def __create_cohort_dataframe(
    connection: RS4,
    meta_omop: OHDSIMetaData,
    cohort_table: str,
    cohort_id: float,
) -> pd.DataFrame:
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
    # Obtain SQL file for standard features
    sql_path = pkg_resources.resource_filename(
        "v6-ohdsi-update-csv", "sql/standard_features.sql"
    )

    # SQL READ
    try:
        raw_sql = sqlrender.read_sql(sql_path)
    except Exception as e:
        error(f"Failed to read SQL file: {e}")
        traceback.print_exc()
        raise e
    info(f"Red SQL file: {sql_path}")

    info("Start query sequence the database")
    df = _query_database(connection, raw_sql, cohort_table, cohort_id, meta_omop)

    # NACHARS
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
    sql = sqlrender.render(
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
    sql = sqlrender.translate(sql, target_dialect="postgresql")

    # QUERY
    info("Querying the database")
    try:
        data_r = database_connector.query_sql(connection, sql)
    except Exception as e:
        error(f"Failed to query the database: {e}")
        traceback.print_exc()
        with open("errorReportSql.txt", "r") as f:
            error(f.read())

    info("Convert")
    # CONVERT
    return ohdsi_common.convert_from_r(data_r)
