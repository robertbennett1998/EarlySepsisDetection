DROP TABLE IF EXISTS mimiciii.sepsis_patients;
CREATE TABLE mimiciii.sepsis_patients AS
(
    SELECT  adm.subject_id, adm.hadm_id, adm.admittime, adm.diagnosis,
            icu.icustay_id, icu.intime, d.icd9_code
    FROM mimiciii.admissions adm

    INNER JOIN mimiciii.icustays icu
    ON adm.hadm_id = icu.hadm_id

    LEFT JOIN mimiciii.diagnoses_icd d
    ON adm.hadm_id = d.hadm_id AND  (   d.icd9_code = '99590' OR  
                                        d.icd9_code = '99591' OR  
                                        d.icd9_code = '99592' OR  
                                        d.icd9_code = '99593' OR  
                                        d.icd9_code = '99594' OR  
                                        d.icd9_code = '99595'
                                    )

    WHERE   icu.age_at_admission >= 18 AND --where the patient is over 18 and there is a entry in the transfares to the MICU
            EXISTS (    
                        SELECT t.hadm_id, t.curr_careunit 
                        FROM mimiciii.transfers t 
                        WHERE t.hadm_id = adm.hadm_id AND t.curr_careunit = 'MICU'
                    )

    GROUP BY    adm.subject_id, adm.hadm_id, icu.icustay_id, adm.admittime, adm.diagnosis, icu.intime, d.icd9_code
    ORDER BY    adm.subject_id, adm.hadm_id, icu.intime, d.icd9_code
);