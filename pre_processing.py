# %% Imports
import math

import numpy
import pandas as pd
import os
import data_extraction as de
import time

# %% Create file paths
working_directory = os.getcwd()
chart_events_csv_path = os.path.join(working_directory, "sepsis_patients_chart_events.csv")
lab_events_csv_path = os.path.join(working_directory, "sepsis_patients_lab_events.csv")
sepsis_patients_chart_events_flattened_path = os.path.join(working_directory,
                                                           "sepsis_patients_chart_events_flattened.csv")
sepsis_patients_lab_events_flattened_path = os.path.join(working_directory, "sepsis_patients_lab_events_flattened.csv")
sepsis_patient_flattened_data_csv_path = os.path.join(working_directory, "sepsis_patient_flattened_data.csv")
sepsis_patients_with_sirs_cond_path = os.path.join(working_directory, "sepsis_patients_with_sirs_cond.csv")
sepsis_patients_with_no_sirs_cond_path = os.path.join(working_directory, "sepsis_patients_with_no_sirs_cond.csv")
sepsis_patients_csv_path = os.path.join(working_directory, "sepsis_patients.csv")

# %% Flatten function definitions
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
    chart_event_body_temp_f_data_source_1 = 678
    chart_event_body_temp_f_data_source_2 = 223761

    chart_event_respiratory_rate_data_source_1 = 618
    chart_event_respiratory_rate_data_source_2 = 220210

    chart_event_arterial_paco2_data_source_1 = 778
    chart_event_arterial_paco2_data_source_2 = 227038

    chart_event_systolic_blood_pressure_data_source_1 = 51
    chart_event_systolic_blood_pressure_data_source_2 = 442
    chart_event_systolic_blood_pressure_data_source_3 = 455
    chart_event_systolic_blood_pressure_data_source_4 = 6701
    chart_event_systolic_blood_pressure_data_source_5 = 220179
    chart_event_systolic_blood_pressure_data_source_6 = 220050

    chart_event_diastolic_blood_pressure_data_source_1 = 8368
    chart_event_diastolic_blood_pressure_data_source_2 = 8440
    chart_event_diastolic_blood_pressure_data_source_3 = 8441
    chart_event_diastolic_blood_pressure_data_source_4 = 8555
    chart_event_diastolic_blood_pressure_data_source_5 = 220180
    chart_event_diastolic_blood_pressure_data_source_6 = 220051

    chart_event_blood_oxygen_saturation_data_source_1 = 646
    chart_event_blood_oxygen_saturation_data_source_2 = 220277

    metavision_to_careview_item_id_map = {
        chart_event_hr_data_source_2: chart_event_hr_data_source_1,
        chart_event_body_temp_data_source_2: chart_event_body_temp_data_source_1,
        chart_event_body_temp_f_data_source_2: chart_event_body_temp_f_data_source_1,
        chart_event_respiratory_rate_data_source_2: chart_event_respiratory_rate_data_source_1,
        chart_event_arterial_paco2_data_source_2: chart_event_arterial_paco2_data_source_1,
        # chart_event_systolic_blood_pressure_data_source_2: chart_event_systolic_blood_pressure_data_source_1,
        # chart_event_diastolic_blood_pressure_data_source_2: chart_event_diastolic_blood_pressure_data_source_1,
        chart_event_blood_oxygen_saturation_data_source_2: chart_event_blood_oxygen_saturation_data_source_1
    }

    itemid_to_name_map = {
        chart_event_hr_data_source_1: "heart_rate",
        chart_event_body_temp_data_source_1: "body_temp",
        chart_event_body_temp_f_data_source_1: "body_temp_f",
        chart_event_respiratory_rate_data_source_1: "respiratory_rate",
        chart_event_arterial_paco2_data_source_1: "paco2",
        chart_event_systolic_blood_pressure_data_source_1: "systolic_blood_pressure_1",
        chart_event_systolic_blood_pressure_data_source_2: "systolic_blood_pressure_2",
        chart_event_systolic_blood_pressure_data_source_3: "systolic_blood_pressure_3",
        chart_event_systolic_blood_pressure_data_source_4: "systolic_blood_pressure_4",
        chart_event_systolic_blood_pressure_data_source_5: "systolic_blood_pressure_5",
        chart_event_systolic_blood_pressure_data_source_6: "systolic_blood_pressure_6",
        chart_event_diastolic_blood_pressure_data_source_1: "diastolic_blood_pressure_1",
        chart_event_diastolic_blood_pressure_data_source_2: "diastolic_blood_pressure_2",
        chart_event_diastolic_blood_pressure_data_source_3: "diastolic_blood_pressure_3",
        chart_event_diastolic_blood_pressure_data_source_4: "diastolic_blood_pressure_4",
        chart_event_diastolic_blood_pressure_data_source_5: "diastolic_blood_pressure_5",
        chart_event_diastolic_blood_pressure_data_source_6: "diastolic_blood_pressure_6",
        chart_event_blood_oxygen_saturation_data_source_1: "blood_oxygen_saturation",
    }

    start_time = time.time()
    ce_icu_stay_data = pd.read_csv(chart_events_csv_path, parse_dates=["charttime", "intime", "admittime"],
                                   dtype={"subject_id": int, "hadm_id": int, "icustay_id": int, "itemid": int,
                                          "icd9_code": str}, nrows=nrows, low_memory=False)

    ce_icu_stay_data["itemid"] = ce_icu_stay_data["itemid"].replace(metavision_to_careview_item_id_map)
    ce_icu_stay_data = ce_icu_stay_data.join(
        pd.pivot(ce_icu_stay_data, columns="itemid", values="value"))  # move unique item ids into there own columns

    ce_icu_stay_data = ce_icu_stay_data.rename(itemid_to_name_map,
                                               axis=1)  # rename the item ids to what they actual represent

    ce_icu_stay_data = ce_icu_stay_data.rename(itemid_to_name_map,
                                               axis=1)  # rename the item ids to what they actual represent
    ce_icu_stay_data = ce_icu_stay_data.drop(["itemid", "value", "valueuom"],
                                             axis=1)  # drop the item, value and valueuom columns

    ce_icu_stay_data["icd9_code"] = ce_icu_stay_data["icd9_code"].fillna(
        0)  # fill the na with a number, we aren't interested in any other diagnosis and dont want the overhead of any none sepsis codes

    def merge_systolic_blood_pressure(x):
        total = 0.0
        n = 0.0
        for i in range(1, 6 + 1):
            column = "systolic_blood_pressure_" + str(i)
            if column in x.keys() and pd.notna(x[column]):
                total += x[column]
                n += 1

        if n == 0:
            return numpy.nan

        return total / n

    def merge_diastolic_blood_pressure(x):
        total = 0.0
        n = 0.0
        for i in range(1, 6 + 1):
            column = "diastolic_blood_pressure_" + str(i)
            if column in x.keys() and pd.notna(x[column]):
                total += x[column]
                n += 1

        if n == 0:
            return numpy.nan

        return total / n

    def bp_merge_group_by(grp, f, c):
        grp[c] = grp.apply(f, axis=1)
        # print(grp)
        return grp

    ce_icu_stay_data = ce_icu_stay_data.groupby("hadm_id").apply(bp_merge_group_by, merge_systolic_blood_pressure, "systolic_blood_pressure")
    ce_icu_stay_data = ce_icu_stay_data.groupby("hadm_id").apply(bp_merge_group_by, merge_diastolic_blood_pressure, "diastolic_blood_pressure")

    columns_to_drop =   ["systolic_blood_pressure_1", "systolic_blood_pressure_2", "systolic_blood_pressure_3",
                         "systolic_blood_pressure_4", "systolic_blood_pressure_5", "systolic_blood_pressure_6",
                         "diastolic_blood_pressure_1", "diastolic_blood_pressure_2", "diastolic_blood_pressure_3",
                         "diastolic_blood_pressure_4", "diastolic_blood_pressure_5", "diastolic_blood_pressure_6"]

    for column_to_drop in columns_to_drop:
        if column_to_drop in ce_icu_stay_data.columns:
            ce_icu_stay_data = ce_icu_stay_data.drop(columns=column_to_drop)

    aggergate_columns = dict()
    for key, val in itemid_to_name_map.items():
        if val in ce_icu_stay_data.columns:
            aggergate_columns[val] = ["min", "mean", "max"]

    aggergate_columns["systolic_blood_pressure"] = ["min", "mean", "max"]
    aggergate_columns["diastolic_blood_pressure"] = ["min", "mean", "max"]

    ce_icu_stay_data = ce_icu_stay_data.groupby(
        ["subject_id", "hadm_id", "icustay_id", "admittime", "intime", "icd9_code",
         pd.Grouper(key="charttime", freq="1h")]).agg(aggergate_columns)

    if fill_na:
        ce_icu_stay_data = ce_icu_stay_data.groupby(["icustay_id"]).transform(
            lambda x: x.fillna(method="ffill").fillna(method="bfill"))

    ce_icu_stay_data.columns = ['_'.join(col).strip() for col in
                                ce_icu_stay_data.columns.values]  # flatten multi index columns

    ce_icu_stay_data = ce_icu_stay_data.reset_index()

    f_to_c_min = (ce_icu_stay_data["body_temp_f_min"] - 32) * (5 / 9)
    f_to_c_mean = (ce_icu_stay_data["body_temp_f_mean"] - 32) * (5 / 9)
    f_to_c_max = (ce_icu_stay_data["body_temp_f_max"] - 32) * (5 / 9)

    ce_icu_stay_data["body_temp_min"] = ce_icu_stay_data["body_temp_min"].fillna(f_to_c_min)
    ce_icu_stay_data["body_temp_mean"] = ce_icu_stay_data["body_temp_mean"].fillna(f_to_c_mean)
    ce_icu_stay_data["body_temp_max"] = ce_icu_stay_data["body_temp_max"].fillna(f_to_c_max)

    ce_icu_stay_data = ce_icu_stay_data.drop(columns=["body_temp_f_min", "body_temp_f_mean", "body_temp_f_max"])

    ce_icu_stay_data = ce_icu_stay_data.rename({"intime": "icu_admittime", "admittime": "hospital_admittime"}, axis=1)
    ce_icu_stay_data["time_since_hospital_admit"] = (
                ce_icu_stay_data["charttime"] - ce_icu_stay_data["hospital_admittime"]).dt.total_seconds().apply(
        lambda x: x / (60 * 2))
    ce_icu_stay_data["time_since_icu_admit"] = (
                ce_icu_stay_data["charttime"] - ce_icu_stay_data["icu_admittime"]).dt.total_seconds().apply(
        lambda x: x / (60 * 2))
    ce_icu_stay_data["time_between_admit_and_icu"] = (
                ce_icu_stay_data["icu_admittime"] - ce_icu_stay_data["hospital_admittime"]).dt.total_seconds().apply(
        lambda x: x / (60 * 2))

    print(time.time() - start_time, "seconds to process chart event data.")

    if type(write_to_file) is str:
        ce_icu_stay_data.to_csv(write_to_file)

    return ce_icu_stay_data

def flatten_lab_events(lab_events_csv_path, write_to_file=False, fill_na=True, nrows=None):
    # 51301 white blood cells
    # 51144 Bands (immature white blood cells - indicate an inflamatory response - 3-5% is normal) 
    # 50820 blood ph

    lab_event_white_blood_cells = 51301
    lab_event_bands = 51144
    lab_event_blood_ph = 50820

    itemid_to_name_map = {
        lab_event_white_blood_cells: "white_blood_cells_k_per_uL",
        lab_event_bands: "immature_bands_percentage",
        lab_event_blood_ph: "blood_ph"
    }

    start_time = time.time()
    le_icu_stay_data = pd.read_csv(lab_events_csv_path, parse_dates=["charttime"],
                                   dtype={"subject_id": int, "hadm_id": int, "icustay_id": int, "itemid": int},
                                   nrows=nrows, low_memory=False)
    le_icu_stay_data["value"] = le_icu_stay_data.value.str.extract(r"(\-?\d+(?:.\d+)?)",
                                                                   expand=False)  # TODO: Something smarter, some WBC readings are <1.0 - Probs impute 0? This takes the number part of <1.0

    le_icu_stay_data = le_icu_stay_data.join(
        pd.pivot(le_icu_stay_data, columns="itemid", values="value"))  # move unique item ids into there own columns
    le_icu_stay_data = le_icu_stay_data.rename(itemid_to_name_map,
                                               axis=1)  # rename the item ids to what they actual represent
    le_icu_stay_data = le_icu_stay_data.drop(["itemid", "value", "valueuom"],
                                             axis=1)  # drop the item, value and valueuom columns

    le_icu_stay_data["immature_bands_percentage"] = le_icu_stay_data["immature_bands_percentage"].astype(float)
    le_icu_stay_data["white_blood_cells_k_per_uL"] = le_icu_stay_data["white_blood_cells_k_per_uL"].astype(float)
    le_icu_stay_data["blood_ph"] = le_icu_stay_data["blood_ph"].astype(float)

    aggergate_columns = dict()
    for key, val in itemid_to_name_map.items():
        if val in le_icu_stay_data.columns:
            aggergate_columns[val] = ["min", "mean", "max"]

    le_icu_stay_data = le_icu_stay_data.groupby(
        ["subject_id", "hadm_id", "icustay_id", pd.Grouper(key="charttime", freq="1h")]).agg(aggergate_columns)
    if fill_na:
        le_icu_stay_data = le_icu_stay_data.groupby(["icustay_id"]).transform(
            lambda x: x.fillna(method="ffill").fillna(method="bfill"))

    le_icu_stay_data.columns = ['_'.join(col).strip() for col in
                                le_icu_stay_data.columns.values]  # flatten multi index columns

    le_icu_stay_data = le_icu_stay_data.reset_index()

    print(time.time() - start_time, "seconds to process lab event data.")

    if type(write_to_file) is str:
        le_icu_stay_data.to_csv(write_to_file)

    return le_icu_stay_data

def detect_sirs_condition(data):
    # SIRS conditions
    # Temperature	<36 °C (96.8 °F) or >38 °C (100.4 °F)
    # Heart rate	>90/min
    # Respiratory rate	>20/min or PaCO2<32 mmHg (4.3 kPa)
    # WBC	<4x109/L (<4000/mm³), >12x109/L (>12,000/mm³), or 10% bands - Our data measures WBC in k/uL
    sirs_cond_hr = (data["heart_rate_mean"] > 90)
    sirs_cond_rr = ((data["respiratory_rate_mean"] > 20) | (data["paco2_mean"] < 32))
    sirs_cond_bt = ((data["body_temp_mean"] < 36) | (data["body_temp_mean"] > 38))
    sirs_cond_wbc = ((data["white_blood_cells_k_per_uL_mean"] < 4) | (data["white_blood_cells_k_per_uL_mean"] > 12) | (
                data["immature_bands_percentage_mean"] > 10))

    # meet at least two conditions
    sirs_cond_indicies = (
            (sirs_cond_hr & sirs_cond_rr) |
            (sirs_cond_hr & sirs_cond_bt) |
            (sirs_cond_hr & sirs_cond_wbc) |
            (sirs_cond_rr & sirs_cond_bt) |
            (sirs_cond_rr & sirs_cond_wbc) |
            (sirs_cond_bt & sirs_cond_wbc)
    )
    return sirs_cond_indicies

# %% Read SIRS and Non-SIRS patient data or extract SIRS and Non-SIRS patients
if os.path.exists(sepsis_patients_with_sirs_cond_path) and os.path.exists(sepsis_patients_with_no_sirs_cond_path):
    sirs_cond_icu_patients = pd.read_csv(sepsis_patients_with_sirs_cond_path)
    no_sirs_cond_icu_patients = pd.read_csv(sepsis_patients_with_no_sirs_cond_path)
else:
    # Read the sepsis patient data or join lab events and chart events
    if os.path.exists(sepsis_patient_flattened_data_csv_path):
        sepsis_patient_flattened_data = pd.read_csv(sepsis_patient_flattened_data_csv_path, dtype={"icd9_code": str})
    else:
        # extract chart events
        if not os.path.exists(chart_events_csv_path):
            de.ExtractSepsisChartEvents(chart_events_csv_path)

        if not os.path.exists(sepsis_patients_chart_events_flattened_path):
            ce_data = flatten_chart_events(chart_events_csv_path, sepsis_patients_chart_events_flattened_path,
                                           fill_na=True)
        else:
            ce_data = pd.read_csv(sepsis_patients_chart_events_flattened_path)

        # extract lab events
        if not os.path.exists(lab_events_csv_path):
            de.ExtractSepsisLabEvents(lab_events_csv_path)

        if not os.path.exists(sepsis_patients_lab_events_flattened_path):
            le_data = flatten_lab_events(lab_events_csv_path, sepsis_patients_lab_events_flattened_path, fill_na=True)
        else:
            le_data = pd.read_csv(sepsis_patients_lab_events_flattened_path)

        sepsis_patient_flattened_data = ce_data.merge(le_data, on=["hadm_id", "charttime", "subject_id", "icustay_id"],
                                                      how="left")
        ce_data = None
        le_data = None
        sepsis_patient_flattened_data = sepsis_patient_flattened_data.groupby(by="icustay_id").apply(
            lambda x: x.fillna(method="ffill").fillna(method="bfill")).reset_index()
        sepsis_patient_flattened_data = sepsis_patient_flattened_data.dropna(axis=1, how="all")
        #sepsis_patient_flattened_data = sepsis_patient_flattened_data.dropna(0)

        sepsis_patient_flattened_data.to_csv(sepsis_patient_flattened_data_csv_path)

    time_filtered = sepsis_patient_flattened_data[((sepsis_patient_flattened_data["time_since_hospital_admit"] >= 0) & (
                sepsis_patient_flattened_data["time_since_hospital_admit"] <= 4)) | ((sepsis_patient_flattened_data[
                                                                                          "time_since_icu_admit"] >= 0) & (
                                                                                                 sepsis_patient_flattened_data[
                                                                                                     "time_since_icu_admit"] <= 1))]

    sirs_cond_indicies = detect_sirs_condition(time_filtered)

    sirs_cond_icu_stays = time_filtered[sirs_cond_indicies]["icustay_id"].unique()
    time_filtered = None

    sirs_cond_icu_patients = sepsis_patient_flattened_data[
        sepsis_patient_flattened_data["icustay_id"].isin(sirs_cond_icu_stays)]
    sirs_cond_icu_patients.to_csv(sepsis_patients_with_sirs_cond_path)

    no_sirs_cond_icu_patients = sepsis_patient_flattened_data[
        ~sepsis_patient_flattened_data["icustay_id"].isin(sirs_cond_icu_stays)]
    no_sirs_cond_icu_patients.to_csv(sepsis_patients_with_no_sirs_cond_path)

print("Patients with SIRS condition within first hour of hospital admission or first four hours of ICU admittence:",
      sirs_cond_icu_patients["icustay_id"].nunique())
print("Patients without SIRS condition within first hour of hospital admission or first four hours of ICU admittence:",
      no_sirs_cond_icu_patients["icustay_id"].nunique())


def five_hours_sirs(hadm_data):
    i = 0
    k = 0
    h5 = False
    sirs_cond_indicies = detect_sirs_condition(hadm_data)
    for value in sirs_cond_indicies:
        if value:
            i += 1
        else:
            if i >= 5:
                i = 0
                h5 = True
                k -= 5
                break
            i = 0

        k += 1

    hadm_data["continuous_sirs"] = hadm_data["icustay_id"].transform(lambda x: h5)
    hadm_data["time_zero"] = hadm_data["icustay_id"].transform(lambda x: k)

    return hadm_data


def get_pre_episode_data(hadm_data):
    i = 0
    k = 0
    h5 = False
    sirs_cond_indicies = detect_sirs_condition(hadm_data)
    for value in sirs_cond_indicies:
        if value:
            i += 1
        else:
            if i >= 5:
                i = 0
                h5 = True
                k -= 5
                break
            i = 0

        k += 1

    hadm_data["continuous_sirs"] = hadm_data["icustay_id"].transform(lambda x: h5)

    return hadm_data.iloc[k:k+5]


no_sirs_cond_icu_patients = no_sirs_cond_icu_patients.groupby(["icustay_id"]).apply(get_pre_episode_data)

no_sirs_cond_icu_patients["icd9_code"] = no_sirs_cond_icu_patients["icd9_code"].astype(str)
no_sirs_cond_icu_patients["acquired_sepsis"] = (
            no_sirs_cond_icu_patients["continuous_sirs"] & no_sirs_cond_icu_patients["icd9_code"].transform(
        lambda x: x.startswith("9959")))

no_sirs_cond_icu_patients = no_sirs_cond_icu_patients.drop(
    columns=["continuous_sirs", "index", "time_since_hospital_admit", "time_since_icu_admit",
             "time_between_admit_and_icu"])

no_sirs_cond_icu_patients = no_sirs_cond_icu_patients.dropna(axis=0, how="any")

print(no_sirs_cond_icu_patients[(no_sirs_cond_icu_patients["acquired_sepsis"])]["icustay_id"].nunique(),
      "admissions resulted in hospital acquired sepsis.", no_sirs_cond_icu_patients["icustay_id"].nunique() -
      no_sirs_cond_icu_patients[(no_sirs_cond_icu_patients["acquired_sepsis"])]["icustay_id"].nunique(),
      "admissions didn't result in hospital acquired sepsis.")
no_sirs_cond_icu_patients.to_csv(sepsis_patients_csv_path)
