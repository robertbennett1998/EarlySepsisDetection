DROP TABLE IF EXISTS mimiciii.sepsis_patients;
CREATE TABLE mimiciii.sepsis_patients AS
(
    SELECT  adm.subject_id, adm.hadm_id, adm.admittime, adm.diagnosis,
            icu.icustay_id, icu.intime
    FROM mimiciii.admissions adm
    INNER JOIN mimiciii.icustays icu
    ON adm.hadm_id = icu.hadm_id
    INNER JOIN mimiciii.diagnoses_icd d
    ON  adm.hadm_id = d.hadm_id AND 
        (d.icd9_code = '99591' OR d.icd9_code = '99592') -- sepsis or septic shock diagnosis
    WHERE icu.age_at_admission >= 18
    GROUP BY    adm.subject_id, adm.hadm_id, icu.icustay_id, adm.admittime, adm.diagnosis, icu.intime
    ORDER BY    adm.subject_id, adm.hadm_id, icu.icustay_id, adm.admittime, adm.diagnosis, icu.intime
);