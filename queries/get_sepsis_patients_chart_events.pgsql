/*
    * CHART EVENTS *
    * 211 - carevue - heart rate bpm 
    * 22045 - metavision - heart rate bpm
    * 676 - carevue - temp in c
    * 223762 - metavision - temp in c
    * 618 - carevue - respiratory rate
    * 220210 - metavision - respiratory rate
    * 778 - carevue - Arterial PaCO2
    * 227038 - metavision - Arterial PaCO2
*/
SELECT  sp.subject_id, sp.hadm_id, sp.icustay_id, sp.admittime, sp.intime,
        ce.itemid, ce.value, ce.valueuom, ce.charttime, sp.icd9_code
FROM mimiciii.sepsis_patients sp
INNER JOIN mimiciii.chartevents ce
ON sp.hadm_id = ce.hadm_id AND (ce.itemid = '211'       OR -- HR
                                        ce.itemid = '220045'    OR -- HR
                                        ce.itemid = '676'       OR -- TEMP C
                                        ce.itemid = '223762'    OR -- TEMP C
                                        ce.itemid = '678'       OR -- TEMP F
                                        ce.itemid = '223761'    OR -- TEMP F
                                        ce.itemid = '618'       OR -- respiratory rate
                                        ce.itemid = '220210'    OR -- respiratory rate
                                        ce.itemid = '778'       OR -- Arterial PaCO2
                                        ce.itemid = '227038'       -- Arterial PaCO2
                                        )
/*WHERE sp.subject_id = 21 OR sp.subject_id = 38*/
GROUP BY    sp.subject_id, sp.hadm_id, sp.icustay_id, sp.admittime, sp.intime,
            ce.itemid, ce.value, ce.valueuom, ce.charttime, sp.icd9_code
ORDER BY sp.subject_id, sp.hadm_id, sp.admittime, sp.intime, sp.icustay_id, ce.charttime, sp.icd9_code