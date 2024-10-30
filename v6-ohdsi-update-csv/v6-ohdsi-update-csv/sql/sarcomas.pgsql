/* BlueBerry Query
 *
 * This query is a postgres implementation of the querry required in the BlueBerry
 * project. It extracts the following variables for the cohort of interest:
 *
 * Patient ID
 *     The unique identifier of the patient in the OMOP data source. Mainly used for
 *     coupling the different variables.
 * Sex
 *     The gender of the patient. Either MALE or FEMALE.
 * Status
 *     Weather the patient is DEAD or ALIVE.
 * Survival days
 *     The number of days the patient survived after the cohort start date. In case the
 *     patient is alive at the end of the cohort period, the survival days are
 *     calculated as the number of days between the cohort start date and the cohort
 *     end date.
 * Tumor size
 *     The size of the tumor at the time of the last measurement (as there can be several
 *     measurements available per patient.
 * FNCLCC grade
 *     The FNCLCC grade of the tumor at the time of the last measurement (as there can
 *     be several measurements available per patient.
 * Surgery
 *     A binary variable indicating weather the patient had a surgery or not. (not part
 *     of the specification, but added om that it was testable with the synthetic data)
 * Age
 *     The age of the patient at the time of the cohort start date.
 * Censor
 *     A binary variable indicating weather the patient is censored or not. A patient is
 *     censored if the patient is alive at the end of the cohort period. This variable is
 *     used in survival analysis.
 *
 * TODO
 *      Multifocality,
 *      Completeness of resection,
 *      Tumor rupture
 *      Chemotherapy
 *      Radiotherapy
 *
 * All concept IDs are replaced by their corresponding concept names. So that the user
 * can easily understand the data.
 */

WITH

    --- get all patients in the cohort
    person AS (
        SELECT
            cohort.subject_id as person_id,
            gender_concept.concept_name as sex,
            DATE_PART('year', cohort.cohort_start_date) - DATE_PART('year', person.birth_datetime) as age
        FROM
            results_synthetic.cohort_94_42 cohort
        LEFT JOIN
            omopcdm_synthetic.person person
            ON cohort.subject_id = person.person_id
        LEFT JOIN
            omopcdm_synthetic.concept gender_concept
            ON person.gender_concept_id = gender_concept.concept_id
    ),

    --- get all patients in the cohort and their death information
    death AS (
        SELECT
            cohort.subject_id as person_id,
            CASE WHEN death.death_date IS NOT NULL THEN 1 ELSE 0 END AS censor,
            CASE WHEN death.death_date IS NOT NULL THEN 'DEAD' ELSE 'ALIVE' END AS status,
            COALESCE(
                (death.death_date - cohort.cohort_start_date),
                (cohort.cohort_end_date - cohort.cohort_start_date)
            ) AS survival_days
        FROM
            results_synthetic.cohort_94_42 cohort
        LEFT JOIN
            omopcdm_synthetic.death death
            ON cohort.subject_id = death.person_id
    ),

    --- get all surgery concepts and their decendants sorted by date, newest first
    all_surgery AS (
        SELECT
            cohort.subject_id as person_id,
            procedure_.procedure_concept_id,
            -- concept.concept_name as chemotherapy_type
            ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY procedure_datetime DESC) AS rank
        FROM
            results_synthetic.cohort_94_42 cohort
        LEFT JOIN
            omopcdm_synthetic.procedure_occurrence procedure_
            ON cohort.subject_id = procedure_.person_id
        LEFT JOIN
            omopcdm_synthetic.concept_ancestor ancestor
            ON procedure_.procedure_concept_id = ancestor.descendant_concept_id
        LEFT JOIN
            omopcdm_synthetic.concept concept
            ON ancestor.ancestor_concept_id = concept.concept_id
        WHERE ancestor.ancestor_concept_id IN (
            4301351, -- Surgical procedure
            4315400, -- Local excision
            4217482, -- Amputation
            4181193  -- Limb operation
        )
    ),

    --- select only one surgery per patient
    surgery AS (
        SELECT
            person_id,
            procedure_concept_id
        FROM
            all_surgery
        WHERE rank = 1
    ),

    --- get all tumor size measurements sorted by date, newest first
    all_tumor_size AS (
        SELECT
            cohort.subject_id as person_id,
            measurement.measurement_concept_id,
            measurement.value_as_number as size,
            measurement.measurement_datetime,
            ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY measurement_datetime DESC) AS rank
        FROM
            results_synthetic.cohort_94_42 cohort
        LEFT JOIN
            omopcdm_synthetic.measurement measurement
            ON cohort.subject_id = measurement.person_id
        WHERE measurement.measurement_concept_id = 4265162
    ),
    --- select only one tumor measurement per patient
    tumor AS (
        SELECT
            person_id,
            size
        FROM
            all_tumor_size
        WHERE rank = 1
    ),

    --- get all FNCLCC grade measurements sorted by date, newest first
    all_fnclcc AS (
        SELECT
            cohort.subject_id as person_id,
            measurement.measurement_concept_id,
            measurement.value_as_number as grade,
            measurement.measurement_datetime,
            ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY measurement_datetime DESC) AS rank
        FROM
            results_synthetic.cohort_94_42 cohort
        LEFT JOIN
            omopcdm_synthetic.measurement measurement
            ON cohort.subject_id = measurement.person_id
        WHERE measurement.measurement_concept_id = 4139510
    ),

    --- select only one FNCLCC grade per patient
    fnclcc AS (
        SELECT
            person_id,
            grade
        FROM
            all_fnclcc
        WHERE rank = 1
    )

    --- get all tumor size measurements sorted by date, newest first
    tumor_rupture AS (
        SELECT
            cohort.subject_id as person_id,
            measurement.measurement_concept_id,
            measurement.value_as_number as size,
            measurement.measurement_datetime,
            ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY measurement_datetime DESC) AS rank
        FROM
            results_synthetic.cohort_94_42 cohort
        LEFT JOIN
            omopcdm_synthetic.measurement measurement
            ON cohort.subject_id = measurement.person_id
        WHERE measurement.measurement_concept_id = 36768904 --- Tumor rupture
    ),
    --- select only one tumor measurement per patient
    tumor AS (
        SELECT
            person_id,
            size
        FROM
            all_tumor_size
        WHERE rank = 1
    ),


SELECT
    person.person_id as Patient_ID,
    person.age as Age,
    person.sex as Sex,
    death.censor as Censor,
    death.status as SAVEPOINT,
    death.survival_days as Survival_days,
    CASE WHEN surgery.procedure_concept_id IS NOT NULL THEN 1 ELSE 0 END AS surgery,
    sur
    tumor.size as Tumor_size,
    fnclcc.grade as FNCLCC_grade
FROM
    person
LEFT JOIN
    death
    ON person.person_id = death.person_id
LEFT JOIN
    surgery
    ON person.person_id = surgery.person_id
LEFT JOIN
    tumor
    ON person.person_id = tumor.person_id
LEFT JOIN
    fnclcc
    ON person.person_id = fnclcc.person_id
ORDER BY
    person.person_id
LIMIT 100;