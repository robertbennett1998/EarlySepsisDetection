#%% Imports
import pandas as pd
import os
import data_extraction as de
from tqdm import tqdm
import time
import numpy as np

#%% Create file paths
working_directory = os.getcwd()
chart_events_csv_path = os.path.join(working_directory, "sepsis_patients_chart_events.csv")
lab_events_csv_path = os.path.join(working_directory, "sepsis_patients_lab_events.csv")
sepsis_patients_chart_events_flattened = os.path.join(working_directory, "sepsis_patients_chart_events_flattened.csv")
sepsis_patients_lab_events_flattened = os.path.join(working_directory, "sepsis_patients_lab_events_flattened.csv")
sepsis_patient_flattened_data_csv = os.path.join(working_directory, "sepsis_patient_flattened_data.csv")
sepsis_patients_with_sirs_cond_path = os.path.join(working_directory, "sepsis_patients_with_sirs_cond.csv")
sepsis_patients_with_no_sirs_cond_path = os.path.join(working_directory, "sepsis_patients_with_no_sirs_cond.csv")
temp_csv = os.path.join(working_directory, "temp.csv")

#%% Process chart events
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
    ce_icu_stay_data = pd.read_csv(chart_events_csv_path, parse_dates=["charttime", "intime", "admittime"], dtype={"subject_id": int, "hadm_id": int, "icustay_id": int, "itemid": int }, nrows=nrows, low_memory=False)

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

    print(time.time() - start_time, "seconds to process chart event data.")

    if type(write_to_file) is str:
        ce_icu_stay_data.to_csv(write_to_file)

    return ce_icu_stay_data

if not os.path.exists(chart_events_csv_path):
    de.ExtractSepsisChartEvents(chart_events_csv_path)

if not os.path.exists(sepsis_patients_chart_events_flattened):
    ce_data = flatten_chart_events(chart_events_csv_path, sepsis_patients_chart_events_flattened, fill_na=True)
else:
    ce_data = pd.read_csv(sepsis_patients_chart_events_flattened)

#%% Process lab events
def flatten_lab_events(lab_events_csv_path, write_to_file=False, fill_na=True, nrows=None):
    # 51301 white blood cells
    # 51144 Bands (immature white blood cells - indicate an inflamatory response - 3-5% is normal) 

    lab_event_white_blood_cells = 51301
    lab_event_bands = 51144

    itemid_to_name_map = {
        lab_event_white_blood_cells : "white_blood_cells_k_per_uL",
        lab_event_bands : "immature_bands_percentage"
    }
    
    start_time = time.time()
    le_icu_stay_data = pd.read_csv(lab_events_csv_path, parse_dates=["charttime"], dtype={"subject_id": int, "hadm_id": int, "icustay_id": int, "itemid": int }, nrows=nrows, low_memory=False)
    le_icu_stay_data["value"] = le_icu_stay_data.value.str.extract('(\d+)', expand=False) #TODO: Something smarter, some WBC readings are <1.0 - Probs impute 0? This takes the number part of <1.0

    le_icu_stay_data = le_icu_stay_data.join(pd.pivot(le_icu_stay_data, columns="itemid", values="value")) #move unique item ids into there own columns
    le_icu_stay_data = le_icu_stay_data.rename(itemid_to_name_map, axis=1) #rename the item ids to what they actual represent
    le_icu_stay_data = le_icu_stay_data.drop(["itemid", "value", "valueuom"], axis=1) #drop the item, value and valueuom columns

    le_icu_stay_data["immature_bands_percentage"] = le_icu_stay_data["immature_bands_percentage"].astype(float)
    le_icu_stay_data["white_blood_cells_k_per_uL"] = le_icu_stay_data["white_blood_cells_k_per_uL"].astype(float)

    le_icu_stay_data = le_icu_stay_data.groupby(["subject_id", "hadm_id", "icustay_id", pd.Grouper(key="charttime", freq="1h")]).agg({
                                                                                                                                        "white_blood_cells_k_per_uL": ["min", "mean", "max"],
                                                                                                                                        "immature_bands_percentage": ["min", "mean", "max"]
                                                                                                                                    })
    if fill_na:
        le_icu_stay_data = le_icu_stay_data.groupby(["icustay_id"]).transform(lambda x : x.fillna(method="ffill").fillna(method="bfill"))

    le_icu_stay_data.columns = ['_'.join(col).strip() for col in le_icu_stay_data.columns.values] #flatten multi index columns

    le_icu_stay_data = le_icu_stay_data.reset_index()

    print(time.time() - start_time, "seconds to process lab event data.")

    if type(write_to_file) is str:
        le_icu_stay_data.to_csv(write_to_file)

    return le_icu_stay_data

if not os.path.exists(lab_events_csv_path):
    de.ExtractSepsisLabEvents(lab_events_csv_path)

if not os.path.exists(sepsis_patients_lab_events_flattened):
    le_data = flatten_lab_events(lab_events_csv_path, sepsis_patients_lab_events_flattened, fill_na=True)
else:
    le_data = pd.read_csv(sepsis_patients_lab_events_flattened)

#%% Join lab events and chart events
# if os.path.exists(sepsis_patient_flattened_data_csv):
#     sepsis_patient_flattened_data = pd.read_csv(sepsis_patient_flattened_data_csv)
#     ce_data = None
#     le_data = None
# else:
sepsis_patient_flattened_data = ce_data.merge(le_data, on=["hadm_id", "charttime", "subject_id", "icustay_id"], how="left")
ce_data = None
le_data = None
sepsis_patient_flattened_data = sepsis_patient_flattened_data.groupby(by="icustay_id").apply(lambda x : x.fillna(method="ffill").fillna(method="bfill")).reset_index()
sepsis_patient_flattened_data.to_csv(sepsis_patient_flattened_data_csv)

#%% Extract SIRS and Non-SIRS patients
time_filtered = sepsis_patient_flattened_data[((sepsis_patient_flattened_data["time_since_hospital_admit"] >= 0) & (sepsis_patient_flattened_data["time_since_hospital_admit"] <= 1)) | ((sepsis_patient_flattened_data["time_since_icu_admit"] >= 0) & (sepsis_patient_flattened_data["time_since_icu_admit"] <= 4))]

# SIRS conditions
    # Temperature	<36 °C (96.8 °F) or >38 °C (100.4 °F)
    # Heart rate	>90/min
    # Respiratory rate	>20/min or PaCO2<32 mmHg (4.3 kPa)
    # WBC	<4x109/L (<4000/mm³), >12x109/L (>12,000/mm³), or 10% bands - Our data measures WBC in k/uL
sirs_cond_hr = (time_filtered["heart_rate_max"] > 90)
sirs_cond_rr = ((time_filtered["respiratory_rate_max"] > 20) | (time_filtered["paco2_min"] < 32))
sirs_cond_bt = ((time_filtered["body_temp_min"] < 36) | (time_filtered["body_temp_max"] > 38))
sirs_cond_wbc = ((time_filtered["white_blood_cells_k_per_uL_min"] < 4) | (time_filtered["white_blood_cells_k_per_uL_max"] > 12) |  (time_filtered["immature_bands_percentage_max"] > 10))

#meet at least two conditions

sirs_cond_indicies = (
                (sirs_cond_hr & sirs_cond_rr)   | 
                (sirs_cond_hr & sirs_cond_bt)   | 
                (sirs_cond_hr & sirs_cond_wbc)  | 
                (sirs_cond_rr & sirs_cond_bt)   | 
                (sirs_cond_rr & sirs_cond_wbc)  |
                (sirs_cond_bt & sirs_cond_wbc)
            )

sirs_cond_icu_stays = time_filtered[sirs_cond_indicies]["icustay_id"].unique()

sirs_cond_icu_patients = sepsis_patient_flattened_data[sepsis_patient_flattened_data["icustay_id"].isin(sirs_cond_icu_stays)]
sirs_cond_icu_patients.to_csv(sepsis_patients_with_sirs_cond_path)

no_sirs_cond_icu_patients = sepsis_patient_flattened_data[~sepsis_patient_flattened_data["icustay_id"].isin(sirs_cond_icu_stays)]
no_sirs_cond_icu_patients.to_csv(sepsis_patients_with_no_sirs_cond_path)

print("Patients with SIRS condition within first hour of hospital admission or first four hours of ICU admittence:", sirs_cond_icu_patients["icustay_id"].nunique())
print("Patients without SIRS condition within first hour of hospital admission or first four hours of ICU admittence:", no_sirs_cond_icu_patients["icustay_id"].nunique())