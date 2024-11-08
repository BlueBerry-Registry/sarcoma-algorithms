WITH

    primary_tumor AS (
        SELECT
            episode.person_id,
            episode.episode_id,
            episode.episode_concept_id,
            episode.episode_start_date as diagnosis_date,
            episode.episode_end_date as diagnosis_end_date,
            episode.episode_object_concept_id as diagnosis_concept,
            diagnosis_concept.concept_name as diagnosis
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.episode episode
            ON cohort.subject_id = episode.person_id
        LEFT JOIN
        	@cdm_schema.concept diagnosis_concept
        	ON episode.episode_object_concept_id = diagnosis_concept.concept_id
        WHERE
            episode.episode_concept_id = 32533
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
    ),

    person AS (
        SELECT
            cohort.subject_id as person_id,
            gender_concept.concept_name as sex,

            EXTRACT(YEAR FROM pt.diagnosis_date) - person.year_of_birth as age
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.person person
            ON cohort.subject_id = person.person_id
        LEFT JOIN
            @cdm_schema.concept gender_concept
            ON person.gender_concept_id = gender_concept.concept_id
        LEFT JOIN
            primary_tumor pt
            ON cohort.subject_id = pt.person_id
        {@cohort_id != -1} ? {WHERE cohort_definition_id = @cohort_id}
    ),

    death AS (
        SELECT
            cohort.subject_id as person_id,
            IIF(death.death_date IS NOT NULL, 1, 0) AS censor,
            IIF(death.death_date IS NOT NULL, 'DEAD', 'ALIVE') AS status,
            ISNULL(
                (death.death_date - cohort.cohort_start_date),
                (cohort.cohort_end_date - cohort.cohort_start_date)
            ) AS survival_days
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.death death
            ON cohort.subject_id = death.person_id
        LEFT JOIN
            @cdm_schema.observation_period op
            ON cohort.subject_id = op.person_id
        {@cohort_id != -1} ? {WHERE cohort_definition_id = @cohort_id}
    ),

    surgery AS (
        SELECT
            all_surgeries.person_id,
            all_surgeries.episode_start_date as surgery_date,
            po.procedure_concept_id  as surgery_concept,
            surgery_concept.concept_name as surgery
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY episode.person_id ORDER BY episode.episode_start_date) AS rn
            FROM
                @cdm_schema.episode episode
            LEFT JOIN
                @results_schema.@cohort_table cohort
                ON cohort.subject_id = episode.person_id
            WHERE
                episode.episode_concept_id = 32939
                AND episode.episode_parent_id IN (SELECT primary_tumor.episode_id FROM primary_tumor)
                {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
        ) AS all_surgeries
        LEFT join
            @cdm_schema.episode_event ee
            on all_surgeries.episode_id = ee.episode_id
        LEFT join
            @cdm_schema.procedure_occurrence po
            on ee.event_id = po.procedure_occurrence_id
        LEFT JOIN
            @cdm_schema.concept surgery_concept
            ON po.procedure_concept_id = surgery_concept.concept_id
        WHERE rn = 1
    ),

    tumor_rupture AS (
        SELECT
            cohort.subject_id as person_id,
            measurement.measurement_concept_id
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.measurement measurement
            ON cohort.subject_id = measurement.person_id
        left join
            surgery
            on surgery.person_id = measurement.person_id
        WHERE
            measurement.measurement_concept_id = 36768904
            and surgery.surgery_date = measurement.measurement_date
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
    ),

    resection AS (
        SELECT
            cohort.subject_id as person_id,
            measurement.measurement_concept_id,
            resection_concept.concept_name AS resection,
            IIF(measurement.measurement_concept_id in (1634643,1633801), 'Macroscopically complete', 'Macroscopically incomplete') AS completeness_of_resection
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.measurement measurement
            ON cohort.subject_id = measurement.person_id
        left join
            surgery
            on surgery.person_id = measurement.person_id
        LEFT JOIN
            @cdm_schema.concept resection_concept
            ON measurement.measurement_concept_id = resection_concept.concept_id
        WHERE
            measurement.measurement_concept_id IN (1634643,1633801,1634484)
            AND surgery.surgery_date = measurement.measurement_date
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
    ),

    recurrence AS (
        SELECT
            cohort.subject_id as person_id,
            count(*) as n_recurrence
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.condition_occurrence co
            ON cohort.subject_id = co.person_id
        LEFT JOIN
            primary_tumor
            ON cohort.subject_id = primary_tumor.person_id
        JOIN
            (SELECT
                *
            FROM
                @cdm_schema.CONCEPT c
            JOIN
                @cdm_schema.CONCEPT_ANCESTOR ca
                ON c.concept_id = ca.descendant_concept_id
                AND ca.ancestor_concept_id IN (4097297)
                AND c.invalid_reason IS NULL
            ) AS recurrence_concept
            ON co.condition_concept_id = recurrence_concept.descendant_concept_id
        WHERE
            DATEDIFF(day,primary_tumor.diagnosis_date, co.condition_start_date) > 0
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
        GROUP BY
            cohort.subject_id
    ),

    metastasis AS (
        SELECT
            cohort.subject_id as person_id,
            count(*) as n_metastasis
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.condition_occurrence co
            ON cohort.subject_id = co.person_id
        LEFT JOIN
            primary_tumor
            ON cohort.subject_id = primary_tumor.person_id
        JOIN
            (SELECT
                *
            FROM
                @cdm_schema.CONCEPT c
            JOIN
                @cdm_schema.CONCEPT_ANCESTOR ca
                ON c.concept_id = ca.descendant_concept_id
                AND ca.ancestor_concept_id IN (36769180)
                AND c.invalid_reason IS NULL
            ) AS metastasis_concept
            ON co.condition_concept_id = metastasis_concept.descendant_concept_id
        WHERE
            DATEDIFF(day,primary_tumor.diagnosis_date, co.condition_start_date) > 90
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
        GROUP BY
            cohort.subject_id
    ),

    focality AS (
        SELECT
            cohort.subject_id as person_id,
            measurement.measurement_concept_id,
            upper(focality_concept.concept_name) AS focality
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.measurement measurement
            ON cohort.subject_id = measurement.person_id
        LEFT JOIN
            primary_tumor
            on primary_tumor.person_id = measurement.person_id
        LEFT JOIN
            @cdm_schema.concept focality_concept
            ON measurement.measurement_concept_id = focality_concept.concept_id
        WHERE
            measurement.measurement_concept_id IN (36769933,36769332)
            AND primary_tumor.diagnosis_date = measurement.measurement_date
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
   UNION
    	SELECT
            cohort.subject_id as person_id,
            condition.condition_concept_id,
            upper(focality_concept.concept_name) AS focality
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.condition_occurrence condition
            ON cohort.subject_id = condition.person_id
        LEFT JOIN
            primary_tumor
            on primary_tumor.person_id = condition.person_id
        LEFT JOIN
            @cdm_schema.concept focality_concept
            ON condition.condition_concept_id = focality_concept.concept_id
        WHERE
            condition.condition_concept_id IN (4163998,4163442)
            AND primary_tumor.diagnosis_date = condition.condition_start_date
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
    ),
    tumor_size AS (
        SELECT
            all_tumor_size.subject_id AS person_id,
            CASE
                WHEN all_tumor_size.unit_concept_id = 8582 THEN all_tumor_size.value_as_number
                WHEN all_tumor_size.unit_concept_id = 8588 THEN all_tumor_size.value_as_number/10
            END AS tumor_size
        FROM (
            SELECT
                cohort.subject_id,
                measurement.value_as_number,
                measurement.unit_concept_id,
                ROW_NUMBER() OVER (PARTITION BY cohort.subject_id ORDER BY
                                CASE
                                    WHEN measurement.unit_concept_id = 8582 THEN measurement.value_as_number
                                    WHEN measurement.unit_concept_id = 8588 THEN measurement.value_as_number/10
                                END DESC) AS rn
            FROM
                @results_schema.@cohort_table cohort
            LEFT JOIN
                @cdm_schema.measurement measurement
                ON cohort.subject_id = measurement.person_id
            LEFT JOIN
                primary_tumor
                ON primary_tumor.person_id = measurement.person_id
                AND primary_tumor.diagnosis_date = measurement.measurement_date
            LEFT JOIN
                surgery
                ON surgery.person_id = measurement.person_id
                AND surgery.surgery_date = measurement.measurement_date
            WHERE
                measurement.measurement_concept_id IN (36768664,36768255) -- Tumor size concepts
                AND (primary_tumor.diagnosis_date IS NOT NULL OR surgery.surgery_date IS NOT NULL)
                {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
        ) AS all_tumor_size
        WHERE rn = 1
    )

SELECT
    person.person_id as Patient_ID,
    person.age as Age,
    CASE
	    WHEN person.age < 18 THEN '<18'
	    WHEN person.age >= 18 and person.age < 60 THEN '18-59'
	    WHEN person.age >= 60 THEN '>59'
    END as Age_Group, -- change with the actual age groups
    person.sex as Sex,
    death.censor as Censor,
    death.status as SAVEPOINT,
    death.survival_days as Survival_days,
    primary_tumor.diagnosis as Primary_diagnosis,
    IIF(surgery.surgery_concept IS NOT NULL, 1, 0) AS surgery_yn,
    surgery.surgery as Surgery,
    IIF(tumor_rupture.measurement_concept_id IS NOT NULL, 1, 0) AS tumor_rupture,
    resection.resection as resection,
    resection.completeness_of_resection as Completeness_of_resection,
    IIF(recurrence.n_recurrence IS NOT NULL, 1, 0) as local_recurrence,
    recurrence.n_recurrence as n_recurrence,
    IIF(metastasis.n_metastasis IS NOT NULL, 1, 0) as distant_metastasis,

    focality.focality as Multifocality,
    tumor_size.tumor_size as Tumor_size_cm
FROM
    person
LEFT JOIN
	primary_tumor
	ON person.person_id = primary_tumor.person_id
LEFT JOIN
    death
    ON person.person_id = death.person_id
LEFT JOIN
    surgery
    ON person.person_id = surgery.person_id
LEFT JOIN
    tumor_rupture
    ON person.person_id = tumor_rupture.person_id
LEFT JOIN
    resection
    ON person.person_id = resection.person_id
LEFT JOIN
    recurrence
    ON person.person_id = recurrence.person_id
LEFT JOIN
    metastasis
    ON person.person_id = metastasis.person_id
LEFT JOIN
    focality
    ON person.person_id = focality.person_id
LEFT JOIN
    tumor_size
    ON person.person_id = tumor_size.person_id