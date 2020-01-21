/*
    * CHART EVENTS *
    * 211 - carevue - heart rate bpm 
    * 22045 - metavision - heart rate bpm
    * 676 - carevue - temp in c
    * 223762 - metavision - temp in c
    * 618 - carevue - respiratory rate
    * 220210 - metavision - respiratory rate
    * 778 - carevue - Arterial PaCO2
    * 227038 - metavision - respiratory rate
*/
SELECT  sp.subject_id, sp.icustay_id, 
        ce.itemid, ce.value, ce.valueuom, ce.charttime
FROM mimiciii.sepsis_patients sp
INNER JOIN mimiciii.chartevents ce
ON sp.subject_id = ce.subject_id AND   (ce.itemid = '211'       OR -- HR
                                        ce.itemid = '220045'    OR -- HR
                                        ce.itemid = '676'       OR -- TEMP
                                        ce.itemid = '223762'    OR -- TEMP
                                        ce.itemid = '618'       OR -- respiratory rate
                                        ce.itemid = '220210'    OR -- respiratory rate
                                        ce.itemid = '778'       OR -- Arterial PaCO2
                                        ce.itemid = '227038'       -- Arterial PaCO2
                                        )
GROUP BY    sp.subject_id, sp.icustay_id,
            ce.itemid, ce.value, ce.valueuom, ce.charttime
ORDER BY sp.subject_id, sp.icustay_id, ce.charttime