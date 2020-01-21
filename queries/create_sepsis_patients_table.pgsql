DROP TABLE IF EXISTS mimiciii.sepsis_patients;
CREATE TABLE mimiciii.sepsis_patients AS
(
    SELECT  p.subject_id, 
            icu.icustay_id, icu.age_at_admission
    FROM mimiciii.patients p
    INNER JOIN mimiciii.icustays icu
    ON p.subject_id = icu.subject_id
    INNER JOIN  mimiciii.diagnoses_icd d
    ON  p.subject_id = d.subject_id AND 
        (d.icd9_code = '99591' OR d.icd9_code = '99592') -- sepsis or septic shock diagnosis
    WHERE icu.age_at_admission >= 18 AND icu.age_at_admission < 300
    GROUP BY    p.subject_id,
                icu.icustay_id, icu.age_at_admission
    ORDER BY    p.subject_id, 
                icu.icustay_id
);