WITH
    --- get primary diagnosis for all patients in the cohort (the date is the reference for some of the other variables)
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
        	@vocabulary_schema.concept diagnosis_concept
        	ON episode.episode_object_concept_id = diagnosis_concept.concept_id
        WHERE
            episode.episode_concept_id = 32533 --- Disease Episode (overarching episode)
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
    ),
    --- get all patients in the cohort
    person AS (
        SELECT
            cohort.subject_id as person_id,
            gender_concept.concept_name as sex,
            DATEPART(YEAR, pt.diagnosis_date) - person.year_of_birth as age
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.person person
            ON cohort.subject_id = person.person_id
        LEFT JOIN
            @vocabulary_schema.concept gender_concept
            ON person.gender_concept_id = gender_concept.concept_id
        LEFT JOIN
            primary_tumor pt
            ON cohort.subject_id = pt.person_id
        {@cohort_id != -1} ? {WHERE cohort_definition_id = @cohort_id}
    ),
    --- get all patients in the cohort and their death information
    death AS (
        SELECT
            cohort.subject_id as person_id,
            CAST(IIF(death.death_date IS NOT NULL, 1, 0) AS BIT) AS censor,
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
            @cdm_schema.observation_period op --- questo vale se ho un unico observation_period, altrimenti dovrei prendere l'ultimo (?)
            ON cohort.subject_id = op.person_id
        {@cohort_id != -1} ? {WHERE cohort_definition_id = @cohort_id}
    ),
    --- get survival from 1 to 10 years
    survival AS (
        SELECT
            death.person_id,
            CAST(IIF(death.survival_days >= 365, 1, 0) AS BIT) AS survival_1yr,
            CAST(IIF(death.survival_days >= 2*365, 1, 0) AS BIT) AS survival_2yr,
            CAST(IIF(death.survival_days >= 3*365, 1, 0) AS BIT) AS survival_3yr,
            CAST(IIF(death.survival_days >= 4*365, 1, 0) AS BIT) AS survival_4yr,
            CAST(IIF(death.survival_days >= 5*365, 1, 0) AS BIT) AS survival_5yr,
            CAST(IIF(death.survival_days >= 6*365, 1, 0) AS BIT) AS survival_6yr,
            CAST(IIF(death.survival_days >= 7*365, 1, 0) AS BIT) AS survival_7yr,
            CAST(IIF(death.survival_days >= 8*365, 1, 0) AS BIT) AS survival_8yr,
            CAST(IIF(death.survival_days >= 9*365, 1, 0) AS BIT) AS survival_9yr,
            CAST(IIF(death.survival_days >= 10*365, 1, 0) AS BIT) AS survival_10yr
        FROM 
            death
    ),
    --- histology group
    histo_group AS (
    	SELECT
	    	primary_tumor.person_id, 
	    	CASE
                WHEN primary_tumor.diagnosis_concept IN (36529541,36532543,36540557,36547895,36550930,36565259,36716490,44500609,44501363,44502347,44502555) THEN '1004 Well-differentiated liposarcoma'
                WHEN primary_tumor.diagnosis_concept IN (36529541,36532543,36540557,36547895,36550930,36565259,36716490,44500609,44501363,44502347,44502555) THEN '1007 Dedifferentiated liposarcoma'
                WHEN primary_tumor.diagnosis_concept IN (36517959,36519685,36527462,36528858,36542197,36548944,36567690,36717566,44500548) THEN '1010 Leiomyosarcoma'
                WHEN primary_tumor.diagnosis_concept IN (36564558,44500681) THEN '1013 Solitary fibrous tumour'
                WHEN primary_tumor.diagnosis_concept IN (36518164,36539077,36542266,36545198,36564186,36565777,36567910,36567978) THEN '1016 MPNST'
                WHEN primary_tumor.diagnosis_concept IN (36517265,36536317,36539821,36557573,36563675,36717542,44500743,44501448,44501970,44502968,44505249) THEN '1019 UPS'
                WHEN primary_tumor.diagnosis_concept IN (36517688,36520993,36521675,36521767,36522306,36522587,36523615,36523729,36526673,36527934,36528528,36528553,36529241,36529898,36529914,36530213,36530702,36532061,36533385,36534511,36534836,36535692,36536209,36536483,36538083,36539258,36539942,36540600,36542848,36543361,36543793,36544017,36544331,36544801,36545011,36545492,36548018,36548485,36550322,36553275,36553366,36553716,36554396,36555105,36555193,36555344,36556092,36556544,36557446,36557554,36559526,36559764,36560560,36560902,36561436,36561836,36561934,36562510,36562642,36562881,36563318,36563461,36563534,36565276,36566540,36567178,42511689,42512026,42512136,42512235,42512243,42512454,42512487,42512587,42512696,42512826,42512845,42512942,44499454,44500553,44500656,44500745,44500820,44501225,44501226,44501367,44501368,44502560,44502563) THEN '1022 Other sarcomas'
                ELSE 'N/A'
	        END AS histology
	    FROM primary_tumor
    ),
    --- get main surgery information
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
                AND episode.episode_start_date = cohort.cohort_start_date
            WHERE
                episode.episode_concept_id = 32939
                AND episode.episode_parent_id IN (SELECT primary_tumor.episode_id FROM primary_tumor) --- get the surgeries related only to the overarching episode considered
                AND episode.episode_object_concept_id NOT IN (
					SELECT 
                        c.concept_id
                    FROM 
                        @vocabulary_schema.concept c
                    JOIN 
                        @vocabulary_schema.concept_ancestor ca 
                        ON c.concept_id = ca.descendant_concept_id
                        AND ca.ancestor_concept_id IN (4311405) -- Biopsy 
                        AND c.invalid_reason IS NULL
						AND c.domain_id = 'Measurement'
				)
                {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
        ) AS all_surgeries
        LEFT join
            @cdm_schema.episode_event ee
            on all_surgeries.episode_id = ee.episode_id
        LEFT join
            @cdm_schema.procedure_occurrence po
            on ee.event_id = po.procedure_occurrence_id
        LEFT JOIN
            @vocabulary_schema.concept surgery_concept
            ON po.procedure_concept_id = surgery_concept.concept_id
        WHERE rn = 1
    ),
    --- get tumor rupture after main surgery
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
            measurement.measurement_concept_id = 36768904 --- Tumor Rupture
            and surgery.surgery_date = measurement.measurement_date
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
    ),
    --- get resection information @ main surgery
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
            @vocabulary_schema.concept resection_concept
            ON measurement.measurement_concept_id = resection_concept.concept_id
        WHERE
            measurement.measurement_concept_id IN (1634643,1633801,1634484) --- R0, R1, R2
            AND surgery.surgery_date = measurement.measurement_date
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
    ),
    --- get local recurrence information
    recurrence AS (
        SELECT
            all_recurrence.subject_id AS person_id,
            all_recurrence.condition_start_date AS recurrence_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY co.person_id ORDER BY co.condition_start_date) AS rn
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
                    @vocabulary_schema.concept c
                JOIN
                    @vocabulary_schema.concept_ancestor ca
                    ON c.concept_id = ca.descendant_concept_id
                    AND ca.ancestor_concept_id IN (4097297) --- Recurrent neoplasm
                    AND c.invalid_reason IS NULL
                ) AS recurrence_concept
                ON co.condition_concept_id = recurrence_concept.descendant_concept_id
            WHERE
                DATEDIFF(day,primary_tumor.diagnosis_date, co.condition_start_date) > 0
                {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
        ) AS all_recurrence
        WHERE rn = 1
    ),
    --- get distant metastasis information
    metastasis AS (
        SELECT
            cohort.subject_id as person_id,
            count(*) as n_metastasis
        FROM
            @results_schema.@cohort_table cohort
        LEFT JOIN
            @cdm_schema.measurement m
            ON cohort.subject_id = m.person_id
        LEFT JOIN
            primary_tumor
            ON cohort.subject_id = primary_tumor.person_id
        JOIN
            (SELECT
                *
            FROM
                @vocabulary_schema.concept c
            JOIN
                @vocabulary_schema.concept_ancestor ca
                ON c.concept_id = ca.descendant_concept_id
                AND ca.ancestor_concept_id IN (36769180) --- Metastasis
                AND c.invalid_reason IS NULL
            ) AS metastasis_concept
            ON m.measurement_concept_id = metastasis_concept.descendant_concept_id
        WHERE
            DATEDIFF(day,primary_tumor.diagnosis_date, m.measurement_date) > 90
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
        GROUP BY
            cohort.subject_id
    ),
    --- get information about focality of tumor (unifocal or multifocal) at diagnosis
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
            @vocabulary_schema.concept focality_concept
            ON measurement.measurement_concept_id = focality_concept.concept_id
        WHERE
            measurement.measurement_concept_id IN (36769933,36769332) --- Unifocal Tumor and Multifocal Tumor
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
            @vocabulary_schema.concept focality_concept
            ON condition.condition_concept_id = focality_concept.concept_id
        WHERE
            condition.condition_concept_id IN (4163998,4163442) --- Unifocal tumor and Multifocal tumor
            AND primary_tumor.diagnosis_date = condition.condition_start_date
            {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
    ),
    --- get tumor size (the greater between diagnosis and surgery)
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
    ),
    --- get tumor grade (if grade after surgery is available, otherwise grade at diagnosis)
    tumor_grade AS (
        SELECT
            all_tumor_grade.subject_id AS person_id,
            all_tumor_grade.grade
        FROM (
            SELECT
                cohort.subject_id,
                measurement.measurement_concept_id,
                grade_concept.concept_name as grade,
                measurement.measurement_date,
                ROW_NUMBER() OVER (PARTITION BY cohort.subject_id ORDER BY measurement.measurement_date DESC) AS rn
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
            left join
                @vocabulary_schema.concept grade_concept
                ON measurement.measurement_concept_id = grade_concept.concept_id
            WHERE
                measurement.measurement_concept_id IN (1634371,1634752,1633749) -- FNCLCC grade
                AND (primary_tumor.diagnosis_date IS NOT NULL OR surgery.surgery_date IS NOT NULL)
                {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
        ) AS all_tumor_grade
        WHERE rn = 1
    ),
    --- Pre-operative radiotherapy
    pre_radio AS (
        SELECT
            all_pre_radio.subject_id AS person_id,
            all_pre_radio.episode_start_date AS pre_radio_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY episode.person_id ORDER BY episode.episode_start_date DESC) AS rn
            FROM
                @cdm_schema.episode episode
            LEFT JOIN
                @results_schema.@cohort_table cohort
                ON cohort.subject_id = episode.person_id
            LEFT JOIN
                primary_tumor
                ON cohort.subject_id = primary_tumor.person_id
            JOIN
                surgery
                on cohort.subject_id = surgery.person_id AND episode.episode_start_date < surgery.surgery_date
            WHERE
                episode.episode_concept_id = 32940
                AND episode.episode_parent_id IN (SELECT primary_tumor.episode_id FROM primary_tumor)
                {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
            ) AS all_pre_radio
        WHERE rn = 1
    ),
    --- Post-operative radiotherapy
    post_radio AS (
        SELECT
            all_post_radio.subject_id AS person_id,
            all_post_radio.episode_start_date as post_radio_date
        FROM (
            SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY episode.person_id ORDER BY episode.episode_start_date) AS rn
                FROM
                    @cdm_schema.episode episode
                LEFT JOIN
                    @results_schema.@cohort_table cohort
                    ON cohort.subject_id = episode.person_id
                LEFT JOIN
                    primary_tumor
                    ON cohort.subject_id = primary_tumor.person_id
                JOIN
                    surgery
                    ON cohort.subject_id = surgery.person_id AND episode.episode_start_date > surgery.surgery_date
                LEFT JOIN
                    recurrence
                    on cohort.subject_id = recurrence.person_id
                WHERE
                    episode.episode_concept_id = 32940
                    AND episode.episode_parent_id IN (SELECT primary_tumor.episode_id FROM primary_tumor) --- get the radiotherapies related only to the overarching episode considered
                    AND episode.episode_start_date < ISNULL(recurrence.recurrence_date, primary_tumor.diagnosis_end_date)
                    {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
        ) AS all_post_radio
        WHERE rn = 1
    ),
    --- Pre-operative chemotherapy
    pre_chemo AS (
        SELECT
            all_pre_chemo.subject_id AS person_id,
            all_pre_chemo.episode_start_date as pre_chemo_date
        FROM (
            SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY episode.person_id ORDER BY episode.episode_start_date DESC) AS rn
                FROM
                    @cdm_schema.episode episode
                LEFT JOIN
                    @results_schema.@cohort_table cohort
                    ON cohort.subject_id = episode.person_id
                LEFT JOIN
                    primary_tumor
                    ON cohort.subject_id = primary_tumor.person_id
                JOIN
                    surgery
                    ON cohort.subject_id = surgery.person_id AND episode.episode_start_date < surgery.surgery_date
                JOIN
                    @cdm_schema.procedure_occurrence po
                    ON po.person_id = cohort.subject_id AND po.procedure_date = episode.episode_start_date AND po.procedure_end_date = episode.episode_end_date
                WHERE
                    episode.episode_concept_id IN (32531,32941) --- Treament regimen or Cancer Drug Treatment
                    AND episode.episode_parent_id IN (SELECT primary_tumor.episode_id FROM primary_tumor)
                    AND po.procedure_concept_id IN (
                        SELECT
                            c.concept_id
                        FROM
                            @vocabulary_schema.concept c
                        JOIN
                            @vocabulary_schema.concept_ancestor ca
                            ON c.concept_id = ca.descendant_concept_id
                            AND ca.ancestor_concept_id IN (4273629) --- Chemotherapy and all descendants
                            AND c.invalid_reason IS NULL
                        )
                    {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
                ) AS all_pre_chemo
            WHERE rn = 1
    ),
    --- Post-operative chemotherapy
    post_chemo AS (
        SELECT
            all_post_chemo.subject_id AS person_id,
            all_post_chemo.episode_start_date AS post_chemo_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY episode.person_id ORDER BY episode.episode_start_date) AS rn
            FROM
                @cdm_schema.episode episode
            LEFT JOIN
                @results_schema.@cohort_table cohort
                ON cohort.subject_id = episode.person_id
            LEFT JOIN
                primary_tumor
                ON cohort.subject_id = primary_tumor.person_id
            JOIN
                surgery
                ON cohort.subject_id = surgery.person_id AND episode.episode_start_date > surgery.surgery_date
            LEFT JOIN
                recurrence
                ON cohort.subject_id = recurrence.person_id
            JOIN
                @cdm_schema.procedure_occurrence po
                ON po.person_id = cohort.subject_id AND po.procedure_date = episode.episode_start_date AND po.procedure_end_date = episode.episode_end_date
            WHERE
                episode.episode_concept_id IN (32531,32941) --- Treament regimen or Cancer Drug Treatment
                AND episode.episode_parent_id IN (SELECT primary_tumor.episode_id FROM primary_tumor)
                AND episode.episode_start_date < ISNULL(recurrence.recurrence_date, primary_tumor.diagnosis_end_date)
                AND po.procedure_concept_id IN (
                    SELECT
                        c.concept_id
                    FROM
                        @vocabulary_schema.concept c
                    JOIN
                        @vocabulary_schema.concept_ancestor ca
                        ON c.concept_id = ca.descendant_concept_id
                        AND ca.ancestor_concept_id IN (4273629) --- Chemotherapy and all descendants
                        AND c.invalid_reason IS NULL)
                {@cohort_id != -1} ? {AND cohort_definition_id = @cohort_id}
            ) AS all_post_chemo
        WHERE rn = 1
    )

SELECT
    person.person_id as Patient_ID,
    person.age as Age,
    CASE
	    WHEN person.age < 18 THEN '<18'
	    WHEN person.age >= 18 and person.age <= 30 THEN '18-30'
        WHEN person.age > 30 and person.age <= 40 THEN '31-40'
        WHEN person.age > 40 and person.age <= 50 THEN '41-50'
        WHEN person.age > 50 and person.age <= 60 THEN '51-60'
        WHEN person.age > 60 and person.age <= 70 THEN '61-70'
        WHEN person.age > 70 and person.age <= 80 THEN '71-80'
	    WHEN person.age > 80 THEN '>80'
        ELSE 'N/A'
    END as Age_Group,
    ISNULL(person.sex, 'N/A') as Sex,
    death.censor as Censor,
    death.status as patient_status,
    death.survival_days as Survival_days,
    survival.survival_1yr,
    survival.survival_2yr,
    survival.survival_3yr,
    survival.survival_4yr,
    survival.survival_5yr,
    survival.survival_6yr,
    survival.survival_7yr,
    survival.survival_8yr,
    survival.survival_9yr,
    survival.survival_10yr,
    ISNULL(primary_tumor.diagnosis, 'N/A') as Primary_diagnosis,
    histo_group.histology as histology,
    CAST(IIF(surgery.surgery_concept IS NOT NULL, 1, 0) AS BIT) AS surgery_yn,
    ISNULL(surgery.surgery, 'N/A') as Surgery,
    CAST(IIF(tumor_rupture.measurement_concept_id IS NOT NULL, 1, 0) AS BIT) AS tumor_rupture,
    ISNULL(resection.resection, 'N/A') as resection,
    ISNULL(resection.completeness_of_resection, 'N/A') as Completeness_of_resection,
    CAST(IIF(recurrence.recurrence_date IS NOT NULL, 1, 0) AS BIT) as local_recurrence,
    CAST(IIF(metastasis.n_metastasis IS NOT NULL, 1, 0) AS BIT) as distant_metastasis,
    ISNULL(focality.focality, 'N/A') as Multifocality,
    tumor_size.tumor_size as Tumor_size,
    ISNULL(tumor_grade.grade, 'N/A') as FNCLCC_grade,
    CAST(IIF(pre_chemo.pre_chemo_date IS NOT NULL, 1, 0) AS BIT) as Pre_operative_chemo,
    CAST(IIF(post_chemo.post_chemo_date IS NOT NULL, 1, 0) AS BIT) as Post_operative_chemo,
    CAST(IIF(pre_radio.pre_radio_date IS NOT NULL, 1, 0) AS BIT) as Pre_operative_radio,
    CAST(IIF(post_radio.post_radio_date IS NOT NULL, 1, 0) AS BIT) as Post_operative_radio
FROM
    person
LEFT JOIN
	primary_tumor
	ON person.person_id = primary_tumor.person_id
LEFT JOIN
    death
    ON person.person_id = death.person_id
LEFT JOIN
    survival
    ON person.person_id = survival.person_id
LEFT JOIN
    histo_group
    ON person.person_id = histo_group.person_id
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
LEFT JOIN
    tumor_grade
    ON person.person_id = tumor_grade.person_id
LEFT JOIN
    pre_chemo
    ON person.person_id = pre_chemo.person_id
LEFT JOIN
    post_chemo
    ON person.person_id = post_chemo.person_id
LEFT JOIN
    pre_radio
    ON person.person_id = pre_radio.person_id
LEFT JOIN
    post_radio
    ON person.person_id = post_radio.person_id