SELECT  sp.subject_id, sp.hadm_id, sp.icustay_id, 
        le.itemid, le.value, le.valueuom, le.charttime
FROM mimiciii.sepsis_patients sp
INNER JOIN mimiciii.labevents le
ON sp.subject_id = le.subject_id AND    (
                                                le.itemid = '51301' OR -- white blood cells
                                                le.itemid = '51144' OR -- Bands (immature white blood cells - indicate an inflamatory response - 3-5% is normal) 
                                                le.itemid = '50820'    -- Ph
                                        )
GROUP BY    sp.subject_id, sp.hadm_id, sp.icustay_id,
            le.itemid, le.value, le.valueuom, le.charttime
ORDER BY sp.subject_id, sp.icustay_id, le.charttime