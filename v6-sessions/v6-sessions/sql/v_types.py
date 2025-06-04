import pandas as pd

from vantage6.types import (
    VIntType,
    VOrdinalType,
    VStringBinaryType,
    VIntBinaryType,
    VCategoricalType,
    VDurationType,
    VDataFrame,
)


def assign_types(df: pd.DataFrame) -> VDataFrame:
    """
    Assign types to the columns of the dataframe.
    """

    df = VDataFrame(df)

    # Patient_ID
    df["PATIENT_ID"].v_type = VIntType(description="Patient ID")

    # Age
    df["AGE"].v_type = VIntType(description="Age", unit="years", min=0, max=120)

    # Tumor_size
    df["TUMOR_SIZE"].v_type = VIntType(description="Tumor Size", unit="mm")

    # Survival_days
    df["SURVIVAL_DAYS"].v_type = VDurationType(
        description="Survival Days since cohort entry",
        unit="days",
        min=0,
    )

    # Age_Group
    df["AGE_GROUP"].v_type = VOrdinalType(
        description="Age Group",
        categories=["<18", "18-30", "31-40", "41-50", "51-60", "61-70", "71-80", ">80"],
    )

    # Sex
    df["SEX"].v_type = VStringBinaryType(
        description="Sex", categories=["MALE", "FEMALE"]
    )

    # Censor
    df["CENSOR"].v_type = VIntBinaryType(
        description="Whether patient is censored",
    )

    # patient_status
    df["PATIENT_STATUS"].v_type = VStringBinaryType(
        description="Patient Status",
        categories=["DEAD", "ALIVE"],
    )

    # survival_xyr
    df["SURVIVAL_1YR"].v_type = VIntBinaryType(description="Survival 1yr")
    df["SURVIVAL_2YR"].v_type = VIntBinaryType(description="Survival 2yr")
    df["SURVIVAL_3YR"].v_type = VIntBinaryType(description="Survival 3yr")
    df["SURVIVAL_4YR"].v_type = VIntBinaryType(description="Survival 4yr")
    df["SURVIVAL_5YR"].v_type = VIntBinaryType(description="Survival 5yr")
    df["SURVIVAL_6YR"].v_type = VIntBinaryType(description="Survival 6yr")
    df["SURVIVAL_7YR"].v_type = VIntBinaryType(description="Survival 7yr")
    df["SURVIVAL_8YR"].v_type = VIntBinaryType(description="Survival 8yr")
    df["SURVIVAL_9YR"].v_type = VIntBinaryType(description="Survival 9yr")
    df["SURVIVAL_10YR"].v_type = VIntBinaryType(description="Survival 10yr")

    # death_xyr
    df["DEATH_1YR"].v_type = VIntBinaryType(description="Death 1yr")
    df["DEATH_2YR"].v_type = VIntBinaryType(description="Death 2yr")
    df["DEATH_3YR"].v_type = VIntBinaryType(description="Death 3yr")
    df["DEATH_4YR"].v_type = VIntBinaryType(description="Death 4yr")
    df["DEATH_5YR"].v_type = VIntBinaryType(description="Death 5yr")
    df["DEATH_6YR"].v_type = VIntBinaryType(description="Death 6yr")
    df["DEATH_7YR"].v_type = VIntBinaryType(description="Death 7yr")
    df["DEATH_8YR"].v_type = VIntBinaryType(description="Death 8yr")
    df["DEATH_9YR"].v_type = VIntBinaryType(description="Death 9yr")
    df["DEATH_10YR"].v_type = VIntBinaryType(description="Death 10yr")

    # Primary_diagnosis
    df["PRIMARY_DIAGNOSIS"].v_type = VCategoricalType(
        description="Primary Diagnosis as stored in the database",
    )

    # histology
    df["HISTOLOGY"].v_type = VCategoricalType(
        description="Histology group",
        categories=[
            "1004/1007 Liposarcoma",
            "1010 Leiomyosarcoma",
            "1013 Solitary fibrous tumour",
            "1016 MPNST",
            "1019 UPS",
            "1022 Other sarcomas",
        ],
    )

    # surgery_yn
    df["SURGERY_YN"].v_type = VIntBinaryType(
        description="Whether patient underwent surgery"
    )

    # Surgery
    df["SURGERY"].v_type = VCategoricalType(description="Which surgery was performed")

    # tumor_rupture
    df["TUMOR_RUPTURE"].v_type = VIntBinaryType(description="Tumor Rupture")

    # resection
    df["RESECTION"].v_type = VCategoricalType(
        description="Result of the resection",
        categories=[
            "R0: No residual tumor",
            "R1: Microscopic residual tumor",
            "R2: Macroscopically incomplete resection",
        ],
    )

    # Completeness_of_resection
    df["COMPLETENESS_OF_RESECTION"].v_type = VCategoricalType(
        description="Completeness of Resection",
        categories=["Macroscopically complete", "Macroscopically incomplete"],
    )

    # local_recurrence
    df["LOCAL_RECURRENCE"].v_type = VIntBinaryType(description="Local Recurrence")
    # distant_metastasis
    df["DISTANT_METASTASIS"].v_type = VIntBinaryType(description="Distant Metastasis")

    # Multifocality
    df["MULTIFOCALITY"].v_type = VCategoricalType(
        description="Multifocality",
        categories=[
            "MULTIFOCAL TUMOR",
            "UNIFOCAL TUMOR",
        ],
    )

    # FNCLCC_grade
    df["FNCLCC_GRADE"].v_type = VStringBinaryType(
        description="FNCLCC Grade",
        categories=["Grade 1 tumor", "Grade 2 tumor", "Grade 3 tumor"],
    )

    # Pre_operative_chemo
    df["PRE_OPERATIVE_CHEMO"].v_type = VIntBinaryType(
        description="Pre-operative Chemotherapy",
    )

    # Post_operative_chemo
    df["POST_OPERATIVE_CHEMO"].v_type = VIntBinaryType(
        description="Post-operative Chemotherapy",
    )

    # Pre_operative_radio
    df["PRE_OPERATIVE_RADIO"].v_type = VIntBinaryType(
        description="Pre-operative Radiation Therapy",
    )

    # Post_operative_radio
    df["POST_OPERATIVE_RADIO"].v_type = VIntBinaryType(
        description="Post-operative Radiation Therapy",
    )

    return df
