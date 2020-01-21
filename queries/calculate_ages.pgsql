ALTER TABLE mimiciii.icustays 
ADD COLUMN IF NOT EXISTS age_at_admission FLOAT(6);

WITH micu_patients AS
(
    SELECT p.subject_id, p.dob, icu.icustay_id, icu.intime
    FROM mimiciii.patients p
    INNER JOIN mimiciii.icustays icu
    ON p.subject_id = icu.subject_id
    GROUP BY p.subject_id, p.dob, icu.icustay_id, icu.intime
    ORDER BY  icu.icustay_id
), first_admission_times AS
(
    SELECT DISTINCT ON (icu.icustay_id) icu.subject_id, icu.dob, icu.icustay_id, MIN(icu.intime) AS first_admittime
    FROM micu_patients icu
    WHERE NOT icu.dob IS NULL
    GROUP BY icu.subject_id, icu.dob, icu.icustay_id
    ORDER BY icu.icustay_id
), first_admit_age AS
(
    SELECT ft.subject_id, ft.dob, ft.first_admittime, ft.icustay_id, MIN(ROUND((cast(ft.first_admittime as date) - cast(dob as date)) / 365.242, 2)) AS age
    FROM first_admission_times ft
    GROUP BY ft.subject_id, ft.dob, ft.first_admittime, ft.icustay_id
    ORDER BY ft.icustay_id
)
UPDATE mimiciii.icustays
SET age_at_admission = first_admit_age.age
FROM first_admit_age
WHERE first_admit_age.icustay_id = mimiciii.icustays.icustay_id