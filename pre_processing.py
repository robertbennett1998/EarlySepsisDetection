import pandas as pd
import os
import data_extraction as de
from tqdm import tqdm
import time

def flatten_chart_events(chart_events_csv_path, write_to_file=False, fill_na=True, nrows=None):
    # 211 - carevue - heart rate bpm 
    # 22045 - metavision - heart rate bpm
    # 676 - carevue - temp in c
    # 223762 - metavision - temp in c
    # 618 - carevue - respiratory rate
    # 220210 - metavision - respiratory rate
    # 778 - carevue - Arterial PaCO2
    # 227038 - metavision - Arterial PaCO2

    chart_event_hr_data_source_1 = 211
    chart_event_hr_data_source_2 = 220045
    chart_event_body_temp_data_source_1 = 676
    chart_event_body_temp_data_source_2 = 223762
    chart_event_respiratory_rate_data_source_1 = 618
    chart_event_respiratory_rate_data_source_2 = 220210
    chart_event_arterial_paco2_data_source_1 = 778
    chart_event_arterial_paco2_data_source_2 = 227038

    metavision_to_careview_item_id_map = {
        chart_event_hr_data_source_2: chart_event_hr_data_source_1,
        chart_event_body_temp_data_source_2: chart_event_body_temp_data_source_1,
        chart_event_respiratory_rate_data_source_2: chart_event_respiratory_rate_data_source_1,
        chart_event_arterial_paco2_data_source_2: chart_event_arterial_paco2_data_source_1
    }

    itemid_to_name_map = {
        chart_event_hr_data_source_1 : "heart_rate",
        chart_event_hr_data_source_2 : "heart_rate_ERROR",
        chart_event_body_temp_data_source_1 : "body_temp",
        chart_event_body_temp_data_source_2 : "body_temp_ERROR",
        chart_event_respiratory_rate_data_source_1 : "respiratory_rate",
        chart_event_respiratory_rate_data_source_2 : "respiratory_rate_ERROR",
        chart_event_arterial_paco2_data_source_1 : "paco2",
        chart_event_arterial_paco2_data_source_2 : "paco2_ERROR",
    }
    
    start_time = time.time()
    ce_icu_stay_data = pd.read_csv(chart_events_csv_path, parse_dates=["charttime", "intime", "admittime"], dtype={"subject_id": int, "hadm_id": int, "icustay_id": int, "itemid": int }, nrows=nrows)

    ce_icu_stay_data["itemid"] = ce_icu_stay_data["itemid"].replace(metavision_to_careview_item_id_map)
    ce_icu_stay_data = ce_icu_stay_data.join(pd.pivot(ce_icu_stay_data, columns="itemid", values="value")) #move unique item ids into there own columns
    ce_icu_stay_data = ce_icu_stay_data.rename(itemid_to_name_map, axis=1) #rename the item ids to what they actual represent
    ce_icu_stay_data = ce_icu_stay_data.drop(["itemid", "value", "valueuom"], axis=1) #drop the item, value and valueuom columns
    ce_icu_stay_data = ce_icu_stay_data.groupby(["subject_id", "hadm_id", "icustay_id", "admittime", "intime", pd.Grouper(key="charttime", freq="1h")]).agg({
                                                                                                                                                                "heart_rate": ["min", "mean", "max"],
                                                                                                                                                                "body_temp": ["min", "mean", "max"],
                                                                                                                                                                "respiratory_rate": ["min", "mean", "max"],
                                                                                                                                                                "paco2": ["min", "mean", "max"]
                                                                                                                                                            })

    if fill_na:
        ce_icu_stay_data = ce_icu_stay_data.groupby(["icustay_id"]).transform(lambda x : x.fillna(method="ffill").fillna(method="bfill"))

    ce_icu_stay_data.columns = ['_'.join(col).strip() for col in ce_icu_stay_data.columns.values] #flatten multi index columns

    ce_icu_stay_data = ce_icu_stay_data.reset_index()

    ce_icu_stay_data = ce_icu_stay_data.rename({"intime": "icu_admittime", "admittime": "hospital_admittime"}, axis=1)
    ce_icu_stay_data["time_since_hospital_admit"] = (ce_icu_stay_data["charttime"] - ce_icu_stay_data["hospital_admittime"]).dt.total_seconds().apply(lambda x : x / (60*2))
    ce_icu_stay_data["time_since_icu_admit"] = (ce_icu_stay_data["charttime"] - ce_icu_stay_data["icu_admittime"]).dt.total_seconds().apply(lambda x : x / (60*2))
    ce_icu_stay_data["time_between_admit_and_icu"] = (ce_icu_stay_data["icu_admittime"] - ce_icu_stay_data["hospital_admittime"]).dt.total_seconds().apply(lambda x : x / (60*2))

    print(time.time() - start_time, "seconds to process the data.")

    if type(write_to_file) is str:
        ce_icu_stay_data.to_csv(write_to_file)

    return ce_icu_stay_data

working_directory = os.getcwd()
lab_events_csv_path = os.path.join(working_directory, "sepsis_patients_lab_events.csv")
chart_events_csv_path = os.path.join(working_directory, "sepsis_patients_chart_events.csv")

sepsis_patients_chart_events_flattened = os.path.join(working_directory, "sepsis_patients_chart_events_flattened.csv")
sepsis_patients_chart_events_flattened_no_fill = os.path.join(working_directory, "sepsis_patients_chart_events_flattened_no_fill.csv")

# if not os.path.exists(lab_events_csv_path):
#     de.ExtractSepsisLabEvents(lab_events_csv_path)
# ce_data = flatten_chart_events(chart_events_csv_path, sepsis_patients_chart_events_flattened_no_fill, False)

if not os.path.exists(chart_events_csv_path):
    de.ExtractSepsisChartEvents(chart_events_csv_path)

if not os.path.exists(sepsis_patients_chart_events_flattened):
    ce_data = flatten_chart_events(chart_events_csv_path, sepsis_patients_chart_events_flattened, fill_na=True)
else:
    ce_data = pd.read_csv(sepsis_patients_chart_events_flattened)

time_filtered = ce_data[((ce_data["time_since_hospital_admit"] >= 0) & (ce_data["time_since_hospital_admit"] <= 1)) | ((ce_data["time_since_icu_admit"] >= 0) & (ce_data["time_since_icu_admit"] <= 4))]

sirs_cond_hr = (time_filtered["heart_rate_max"] > 90)
sirs_cond_rr = (time_filtered["respiratory_rate_max"] > 20)
sirs_cond_paco2 = (time_filtered["paco2_min"] > 32)
sirs_cond_rr = (sirs_cond_paco2 | sirs_cond_rr)
sirs_cond_paco2 = None
sirs_cond_bt_max = (time_filtered["body_temp_max"] > 38)
sirs_cond_bt_min = (time_filtered["body_temp_min"] < 36)
sirs_cond_bt = (sirs_cond_bt_min | sirs_cond_bt_max)
sirs_cond_indicies = (
                (sirs_cond_hr & sirs_cond_rr) |
                (sirs_cond_hr & sirs_cond_bt) |
                (sirs_cond_rr & sirs_cond_bt)
            )

sirs_cond = time_filtered[sirs_cond_indicies]
non_sirs_cond = time_filtered[~sirs_cond_indicies]

s_u = sirs_cond["hadm_id"].nunique()
u = non_sirs_cond["hadm_id"].nunique()
print(s_u, u, s_u + u, time_filtered["hadm_id"].nunique(), ce_data["hadm_id"].nunique())
print(sirs_cond["hadm_id"].nunique())

sirs_cond.to_csv(os.path.join(working_directory, "sirs.csv"))